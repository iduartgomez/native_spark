use std::default::Default;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency, ShuffleDependency};
use crate::error::Result;
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::rdd::co_grouped_rdd::CoGroupedRdd;
use crate::rdd::shuffled_rdd::ShuffledRdd;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};

/// An optimized version of cogroup for set difference/subtraction.
///
/// It is possible to implement this operation with just `cogroup`, but
/// that is less efficient because all of the entries from `rdd2`, for
/// both matching and non-matching values in `rdd1`, are kept in the
/// HashMap until the end.
///
/// With this implementation, only the entries from `rdd1` are kept in-memory,
/// and the entries from `rdd2` are essentially streamed, as we only need to
/// touch each once to decide if the value needs to be removed.
///
/// This is particularly helpful when `rdd1` is much smaller than `rdd2`, as
/// you can use `rdd1`'s partitioner/partition size and not worry about running
/// out of memory because of the size of `rdd2`.
#[derive(Clone, Serialize, Deserialize)]
pub struct SubtractedRdd<K: Data, V: Data, W: Data> {
    #[serde(with = "serde_traitobject")]
    rdd1: Arc<dyn Rdd<Item = (K, V)>>,
    #[serde(with = "serde_traitobject")]
    rdd2: Arc<dyn Rdd<Item = (K, W)>>,
    #[serde(with = "serde_traitobject")]
    part: Box<dyn Partitioner>,
    vals: Arc<RddVals>,
}

impl<K: Data, V: Data, W: Data> SubtractedRdd<K, V, W> {
    pub fn new(
        rdd1: Arc<dyn Rdd<Item = (K, V)>>,
        rdd2: Arc<dyn Rdd<Item = (K, W)>>,
        part: Box<dyn Partitioner>,
    ) -> Self {
        let vals = Arc::new(RddVals::new(rdd1.get_context()));
        SubtractedRdd {
            rdd1,
            rdd2,
            part,
            vals,
        }
    }
}

impl<K, V: Data, W: Data> RddBase for SubtractedRdd<K, V, W>
where
    K: Data + Eq + Hash,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        /*
        def rddDependency[T1: ClassTag, T2: ClassTag](rdd: RDD[_ <: Product2[T1, T2]])
          : Dependency[_] = {
          if (rdd.partitioner == Some(part)) {
            logDebug("Adding one-to-one dependency with " + rdd)
            new OneToOneDependency(rdd)
          } else {
            logDebug("Adding shuffle dependency with " + rdd)
            new ShuffleDependency[T1, T2, Any](rdd, part)
          }
        }
        Seq(rddDependency[K, V](rdd1), rddDependency[K, W](rdd2))
        */

        log::debug!("Adding shuffle dependency in subtracted rdd");
        // vec![Dependency::]
        Dependency::ShuffleDependency(Arc::new(ShuffleDependency::<K, V, K>::new(
            self.get_context().new_shuffle_id(),
            true,
            self.rdd1.get_rdd_base(),
            Arc::new(None),
            self.part.clone(),
        )));

        todo!()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        /*
        val array = new Array[Partition](part.numPartitions)
        for (i <- 0 until array.length) {
        // Each CoGroupPartition will depend on rdd1 and rdd2
        array(i) = new CoGroupPartition(i, Seq(rdd1, rdd2).zipWithIndex.map { case (rdd, j) =>
            dependencies(j) match {
            case s: ShuffleDependency[_, _, _] =>
                None
            case _ =>
                Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
            }
        }.toArray)
        }
        array
        */
        todo!()
    }

    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
        ))
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<K, V: Data, W: Data> Rdd for SubtractedRdd<K, V, W>
where
    K: Data + Eq + Hash,
{
    type Item = (K, V);
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        /*
        val partition = p.asInstanceOf[CoGroupPartition]
        val map = new JHashMap[K, ArrayBuffer[V]]
        def getSeq(k: K): ArrayBuffer[V] = {
          val seq = map.get(k)
          if (seq != null) {
            seq
          } else {
            val seq = new ArrayBuffer[V]()
            map.put(k, seq)
            seq
          }
        }
        def integrate(depNum: Int, op: Product2[K, V] => Unit): Unit = {
          dependencies(depNum) match {
            case oneToOneDependency: OneToOneDependency[_] =>
              val dependencyPartition = partition.narrowDeps(depNum).get.split
              oneToOneDependency.rdd.iterator(dependencyPartition, context)
                .asInstanceOf[Iterator[Product2[K, V]]].foreach(op)

            case shuffleDependency: ShuffleDependency[_, _, _] =>
              val metrics = context.taskMetrics().createTempShuffleReadMetrics()
              val iter = SparkEnv.get.shuffleManager
                .getReader(
                  shuffleDependency.shuffleHandle,
                  partition.index,
                  partition.index + 1,
                  context,
                  metrics)
                .read()
              iter.foreach(op)
          }
        }

        // the first dep is rdd1; add all values to the map
        integrate(0, t => getSeq(t._1) += t._2)
        // the second dep is rdd2; remove all of its keys
        integrate(1, t => map.remove(t._1))
        map.asScala.iterator.map(t => t._2.iterator.map((t._1, _))).flatten
        */
        todo!()
    }
}
