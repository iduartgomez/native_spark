use std::hash::Hash;
use std::sync::Arc;

use crate::context::Context;
use crate::dependency::{Dependency, ShuffleDependency};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::co_grouped_rdd::{CoGroupSplit, CoGroupSplitDep};
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data};
use crate::shuffle::ShuffleFetcher;
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};

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
    // precomputed deps
    deps: Vec<Dependency>,
}

impl<K: Data, V: Data, W: Data> SubtractedRdd<K, V, W>
where
    K: Data + Eq + Hash,
{
    pub fn new(
        rdd1: Arc<dyn Rdd<Item = (K, V)>>,
        rdd2: Arc<dyn Rdd<Item = (K, W)>>,
        part: Box<dyn Partitioner>,
    ) -> Self {
        let vals = Arc::new(RddVals::new(rdd1.get_context()));
        SubtractedRdd {
            deps: Self::make_deps(part.clone(), rdd1.get_rdd_base(), rdd2.get_rdd_base()),
            rdd1,
            rdd2,
            part,
            vals,
        }
    }

    fn make_deps(
        part: Box<dyn Partitioner>,
        rdd1: Arc<dyn RddBase>,
        rdd2: Arc<dyn RddBase>,
    ) -> Vec<Dependency> {
        let ctxt = rdd1.get_context();
        let rdd_dep = |rdd: Arc<dyn RddBase>| {
            // TODO: custom partitioner case, which should use narrow dependency
            log::debug!("Adding shuffle dependency in subtracted rdd");
            Dependency::ShuffleDependency(Arc::new(ShuffleDependency::<K, V, K>::new(
                ctxt.new_shuffle_id(),
                false,
                rdd,
                Arc::new(None),
                part.clone(),
            )))
        };

        vec![rdd_dep(rdd1), rdd_dep(rdd2)]
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
        self.deps.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let num_splits = self.part.get_num_of_partitions();
        let mut splits = Vec::with_capacity(num_splits);
        let deps = self.get_dependencies();
        for i in 0..num_splits {
            let cogrouped_splits: Vec<_> = vec![self.rdd1.get_rdd_base(), self.rdd2.get_rdd_base()]
                .into_iter()
                .enumerate()
                .map(|(j, rdd)| match &deps[j] {
                    Dependency::ShuffleDependency(s) => CoGroupSplitDep::ShuffleCoGroupSplitDep {
                        shuffle_id: s.get_shuffle_id(),
                    },
                    _ => CoGroupSplitDep::NarrowCoGroupSplitDep {
                        rdd: rdd.clone(),
                        split: rdd.splits()[i].clone(),
                    },
                })
                .collect();
            splits.push(Box::new(CoGroupSplit::new(i, cogrouped_splits)) as Box<dyn Split>)
        }
        splits
    }

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
        let split = split
            .downcast::<CoGroupSplit>()
            .or(Err(Error::DowncastFailure("CoGroupSplit")))?;
        let mut map: HashMap<K, Vec<V>> = HashMap::new();
        let deps = self.get_dependencies();
        let integrate = |dep_num: usize, product: &mut dyn FnMut((&K, &V))| -> Result<()> {
            match &deps[dep_num] {
                Dependency::NarrowDependency(_) => todo!(),
                Dependency::ShuffleDependency(dep) => {
                    let fut = ShuffleFetcher::fetch::<K, Vec<Box<dyn AnyData>>>(
                        dep.get_shuffle_id(),
                        split.get_index(),
                    );
                    for (k, c) in futures::executor::block_on(fut)?.into_iter() {
                        c.into_iter().for_each(|v| {
                            let v = v.into_any().downcast::<V>().unwrap();
                            product((&k, &*v));
                        });
                    }
                }
            }
            Ok(())
        };

        // the first dep is rdd1; add all values to the map
        integrate(0, &mut |(k, v)| {
            map.entry(k.clone())
                .or_insert_with(Vec::new)
                .push(v.clone())
        })?;

        // the second dep is rdd2; remove all of its keys
        integrate(1, &mut |(k, _)| {
            map.remove(k);
        })?;

        Ok(Box::new(
            map.into_iter()
                .map(|(k, v)| v.into_iter().map(|v| (k.clone(), v)).collect::<Vec<_>>())
                .flatten(),
        ))
    }
}
