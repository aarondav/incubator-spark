/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io._

import scala.{Array, Serializable, Some}
import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.util.{Utils, AppendOnlyMap, SizeEstimator}

private[spark] sealed trait CoGroupSplitDep extends Serializable

private[spark] case class NarrowCoGroupSplitDep(
    rdd: RDD[_],
    splitIndex: Int,
    var split: Partition
  ) extends CoGroupSplitDep {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

private[spark] case class ShuffleCoGroupSplitDep(shuffleId: Int) extends CoGroupSplitDep

private[spark]
class CoGroupPartition(idx: Int, val deps: Array[CoGroupSplitDep])
  extends Partition with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}


/**
 * A RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * @param rdds parent RDDs.
 * @param part partitioner used to partition the shuffle output.
 */
class CoGroupedRDD[K](@transient var rdds: Seq[RDD[_ <: Product2[K, _]]], part: Partitioner)
  extends RDD[(K, Seq[Seq[_]])](rdds.head.context, Nil) {

  private var serializerClass: String = null

  def setSerializer(cls: String): CoGroupedRDD[K] = {
    serializerClass = cls
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_ <: Product2[K, _]] =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[Any, Any](rdd, part, serializerClass)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId)
          case _ =>
            new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i))
        }
      }.toArray)
    }
    array
  }

  override val partitioner = Some(part)

  class FixedSizeTracker[V <: AnyRef] {
    var sizePerObject: Long = 0
    var numObjects = 0

    def add(value: V) {
      if (sizePerObject == 0) {
        sizePerObject = SizeEstimator.estimate(value)
        logInfo("Size per object: " + sizePerObject)
      }
      numObjects += 1
    }

    def size(): Long = sizePerObject * numObjects
  }

  class BucketedHashSet[K](var numBuckets: Int, var numRdds: Int) {
    type V = Product2[K, Any]
    var buckets = Array.fill(numBuckets)(Array.fill(numRdds)(new ArrayBuffer[V]()))
    val sizeTracker = new FixedSizeTracker[V]()

    def insert(rddIndex: Int, value: V) {
      val bucketIndex = hash(value._1)
      buckets(bucketIndex)(rddIndex).append(value)
      sizeTracker.add(value)
    }

    def hash(value: K): Int = math.abs(value.hashCode()) % numBuckets

    def size = sizeTracker.size()
  }

  case class BucketedHashSetSummary(storedFile: File)

  class ExternalBucketedHashSet(numBuckets: Int, numRdds: Int, maxMemoryMb: Int) {
    type V = Product2[K, Any]
    var curMap = new BucketedHashSet[K](numBuckets, numRdds)
    val storedMaps = new ArrayBuffer[BucketedHashSetSummary]

    def insert(rddIndex: Int, value: V) {
      if (curMap == null) {
        curMap = new BucketedHashSet[K](numBuckets, numRdds)
      }

      curMap.insert(rddIndex, value)
      if (curMap.size >= maxMemoryMb * 1024 * 1024) {
        logInfo("Map exceeds maximum size. Current size: " + (curMap.size / 1024 / 1024) + " MB")
        storeCurMap()
      }
    }

    def iterator: Iterator[(K, Seq[ArrayBuffer[Any]])] = {
      new ExternalIterator()
    }

    trait BucketProvider {
      def nextBucket(): Array[ArrayBuffer[V]]
    }

    class MemoryBucketProvider(map: BucketedHashSet[K]) extends BucketProvider {
      private val it = map.buckets.iterator
      def nextBucket(): Array[ArrayBuffer[V]] = {
        it.next()
      }
    }

    class DiskBucketProvider(bucket: BucketedHashSetSummary) extends BucketProvider {
      val stream = new ObjectInputStream(new FileInputStream(bucket.storedFile))
      def nextBucket(): Array[ArrayBuffer[V]] = {
        stream.readObject().asInstanceOf[Array[ArrayBuffer[V]]]
      }
    }

    class ExternalIterator extends Iterator[(K, Seq[ArrayBuffer[Any]])] {
      var bucketIndex = 0
      val inputStreams = Seq(new MemoryBucketProvider(curMap)) ++
        storedMaps.map(bucket => new DiskBucketProvider(bucket))

      var curIterator: Iterator[(K, Seq[ArrayBuffer[Any]])] = computeNext()

      def computeNext(): Iterator[(K, Seq[ArrayBuffer[Any]])] = {
        val map = new AppendOnlyMap[K, Seq[ArrayBuffer[Any]]]

        val update: (Boolean, Seq[ArrayBuffer[Any]]) => Seq[ArrayBuffer[Any]] = (hadVal, oldVal) => {
          if (hadVal) oldVal else Array.fill(numRdds)(new ArrayBuffer[Any])
        }
        val getSeq = (k: K) => { map.changeValue(k, update) }

        inputStreams.foreach { provider =>
          println("Read next bucket from " + provider)
          val bucket = provider.nextBucket()
          for ((depValues, depNum) <- bucket.zipWithIndex; value <- depValues) {
            value match {
              case (k: K, v: Any) => getSeq(k)(depNum) += v
            }
          }
        }

        bucketIndex += 1
        map.iterator
      }

      def hasNext: Boolean = {
        while (bucketIndex < numBuckets && !curIterator.hasNext) {
          curIterator = computeNext()
        }
        curIterator.hasNext
      }

      def next(): (K, Seq[ArrayBuffer[Any]]) = {
        curIterator.next()
      }
    }

    def storeCurMap() {
      logInfo("Writing map to disk...")
      val file = File.createTempFile("cogroup-rdd-thingy-ma-bobber", "")
      val outStream = new ObjectOutputStream(new FileOutputStream(file))
      for (bucket <- curMap.buckets) {
        println("Writing bucket with size " + bucket.map(_.size).sum)
        outStream.writeObject(bucket)
      }
      outStream.close()
      storedMaps.append(new BucketedHashSetSummary(file))
      curMap = null
    }
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(K, Seq[Seq[_]])] = {
    val numBuckets = System.getProperty("cogroup.numBuckets", "8192").toInt
    val maxMemoryMb = System.getProperty("cogroup.maxMemoryMb", "1024").toInt
    val testOld = System.getProperty("cogroup.testold", "false").toBoolean
    if (testOld) { return oldCompute(s, context) }

    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = split.deps.size
    val map = new ExternalBucketedHashSet(numBuckets /* ??? */, numRdds, maxMemoryMb)
    logInfo("Herro!!")
    logError("Hiro!")
    println("println!")
    val ser = SparkEnv.get.serializerManager.get(serializerClass)
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) => {
        // Read them from the parent
        rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, Any]]].foreach { kv =>
          map.insert(depNum, kv)
        }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        val fetcher = SparkEnv.get.shuffleFetcher
        fetcher.fetch[Product2[K, Any]](shuffleId, split.index, context, ser).foreach { kv =>
          map.insert(depNum, kv)
        }
      }
    }
    new InterruptibleIterator(context, map.iterator)
  }

  def oldCompute(s: Partition, context: TaskContext): Iterator[(K, Seq[Seq[_]])] = {
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = split.deps.size
    // e.g. for `(k, a) cogroup (k, b)`, K -> Seq(ArrayBuffer as, ArrayBuffer bs)
    val map = new AppendOnlyMap[K, Seq[ArrayBuffer[Any]]]

    val update: (Boolean, Seq[ArrayBuffer[Any]]) => Seq[ArrayBuffer[Any]] = (hadVal, oldVal) => {
      if (hadVal) oldVal else Array.fill(numRdds)(new ArrayBuffer[Any])
    }

    val getSeq = (k: K) => {
      map.changeValue(k, update)
    }

    val ser = SparkEnv.get.serializerManager.get(serializerClass)
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) => {
        // Read them from the parent
        rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, Any]]].foreach { kv =>
          getSeq(kv._1)(depNum) += kv._2
        }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        val fetcher = SparkEnv.get.shuffleFetcher
        fetcher.fetch[Product2[K, Any]](shuffleId, split.index, context, ser).foreach {
          kv => getSeq(kv._1)(depNum) += kv._2
        }
      }
    }
    new InterruptibleIterator(context, map.iterator)
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
