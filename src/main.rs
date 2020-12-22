#![feature(map_first_last)]
#![feature(total_cmp)]
use std::cmp::Ordering;

// using `faker` module with locales
use fake::faker::name::raw::*;
use fake::locales::*;

use rand::Rng;

// FIXME: replace with Arc and RwLock when threading
use std::rc::Rc;
use std::cell::RefCell;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
struct Document {
    pub conhash_id: f64,
    pub content: String,
}

impl Ord for Document {
    fn cmp(&self, other: &Self) -> Ordering {
        self.conhash_id.total_cmp(&other.conhash_id)
    }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for Document {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Document { }

/// Shards sorted by their consistent hash shard_key
#[derive(Clone, Debug)]
pub struct ShardInfo {
    pub shard_name: String,
    pub shard_key: f64
}

impl Ord for ShardInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.shard_key.total_cmp(&other.shard_key)
    }
}

impl PartialOrd for ShardInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ShardInfo {
    fn eq(&self, other: &Self) -> bool {
        self.shard_key == other.shard_key
    }
}

impl Eq for ShardInfo { }

fn main() {
    let mut rng = rand::thread_rng();

    // shards sorted by shard_key
    let mut shard_mapping = BTreeMap::new();

    let max_shards = 10;
    let num_keys = 100;
    let num_labels = 4;

    // set up data to shard
    let name_vec = fake::vec![String as Name(EN); num_keys];
    let documents: Vec<Document> = name_vec.iter().
        map(|name| Document { conhash_id: consistent_hash(calculate_hash(name)), content: name.to_string() }).
        collect();

    // set up shards and store
    for shard_no in 0..4 {
        let shard_name = format!("shard_{}", shard_no);

        // per each shard, we want x labels for it
        let shard_labels: Vec<f64> = (0..num_labels).
            map(|_| rng.gen_range(0 as f64, 360 as f64)).
            collect();

        // we set up interior mutability
        let data: Rc<RefCell<BTreeSet<Document>>> = Rc::new(
            RefCell::new(BTreeSet::new()));

        for shard_hash in shard_labels {
            let shard_info = ShardInfo {
                shard_name: shard_name.clone(),
                shard_key: shard_hash
            };
            // the actual data holder
            shard_mapping.insert(shard_info, Rc::clone(&data));
        }
    }

    // immutable borrow to "view" the keys
    let shards_view = shard_mapping.clone();

    for document in documents {
        let mut assign_to: &ShardInfo = &shards_view.
            first_key_value().
            expect("shard_mapping must be populated").0;
        for (shard_info, _data) in shards_view.iter() {
            // as soon as you find next largest value
            // correct shard is counter-clockwise (-1)
            if shard_info.shard_key > document.conhash_id {
                break;
            }
            assign_to = shard_info;
        }
        let data = shard_mapping.get(&assign_to).
            expect("shard_key must be present");
        {
            let mut reference = data.borrow_mut();
            reference.insert(document);
        }
    }
    println!("Finished sharding:\n{:#?}", shard_mapping);

    println!("Increasing the shards");
    for shard_no in 5..max_shards {
        let shard_name = format!("shard_{}", shard_no);

        // per each shard, we want x labels for it
        let shard_labels: Vec<f64> = (0..num_labels).
            map(|_| rng.gen_range(0 as f64, 360 as f64)).
            collect();

        // we set up interior mutability
        let data: Rc<RefCell<BTreeSet<Document>>> = Rc::new(
            RefCell::new(BTreeSet::new()));

        for shard_hash in shard_labels {
            let shard_info = ShardInfo {
                shard_name: shard_name.clone(),
                shard_key: shard_hash
            };
            // the actual data holder
            shard_mapping.insert(shard_info, Rc::clone(&data));
        }
    }

    // immutable borrow to "view" the keys
    let shards_view = shard_mapping.clone();

    println!("Time to reshard!");
    let mut moving = Vec::new();
    for (origin_shard_info, documents) in shards_view.iter() {
        for document in documents.borrow().iter() {
            let mut assign_to: &ShardInfo = origin_shard_info;
            let mut candidate: &ShardInfo = origin_shard_info;
            for (shard_info, _data) in shards_view.iter() {
                // as soon as you find next largest value
                // correct shard is counter-clockwise (-1)
                if shard_info.shard_key > document.conhash_id {
                    assign_to = candidate;
                    break;
                }
                candidate = shard_info;
            }
            if assign_to.shard_name != origin_shard_info.shard_name {
                println!("moving {:?} from {:?} to {:?}",
                    document.clone(),
                    origin_shard_info,
                    assign_to);
                moving.push(
                    (origin_shard_info,
                     assign_to,
                     document.clone())
                );
            }
        }
    }

    for (from, to, e) in moving.clone() {
        let data = shard_mapping.get(&to).
            expect("shard_key must be present");
        {
            let mut reference = data.borrow_mut();
            reference.insert(e.clone());
        }
        let data = shard_mapping.get(&from).
            expect("shard_key must be present");
        {
            let mut reference = data.borrow_mut();
            for ee in reference.clone().iter() {
                if e == *ee {
                    reference.remove(&*ee);
                    break;
                }
            }
        }
    }

    println!("Finished resharding:\n{:#?}", shard_mapping);

    // according to the estimate
    println!("Times data moved: {}. Expected: {}",
        moving.len(), num_keys/max_shards);
}

/// https://doc.rust-lang.org/std/hash/index.html
fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn consistent_hash(hash: u64) -> f64 {
    let max = u64::MAX as f64;
    let angle = (hash as f64 / max) * 360 as f64;
    assert!(angle >= 0 as f64);
    assert!(angle <= 360 as f64);
    angle
}
