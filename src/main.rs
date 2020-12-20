#![feature(map_first_last)]
#![feature(total_cmp)]
use std::cmp::Ordering;

// using `faker` module with locales
use fake::faker::name::raw::*;
use fake::locales::*;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use std::collections::{BTreeMap, HashMap};

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
    // convenience shard_name -> ShardInfo
    let mut shards = HashMap::new();
    // shards sorted by shard_key
    let mut shard_mapping = BTreeMap::new();

    // set up data to shard
    let name_vec = fake::vec![String as Name(EN); 100];
    let mut hashed_names: Vec<f64> = name_vec.iter().map(|name| consistent_hash(calculate_hash(name))).collect();

    // set up shards and store
    for shard_no in 0..14 {
        let shard_name = format!("shard_{}", shard_no);
        let shard_hash = consistent_hash(calculate_hash(&shard_name));
        let shard_info = ShardInfo {
            shard_name: shard_name.clone(),
            shard_key: shard_hash
        };
        // just for convenience
        shards.insert(shard_name.clone(), shard_info.clone());
        // the actual data holder
        let data: Vec<f64> = Vec::new();
        shard_mapping.insert(shard_info, data);
    }

    // immutable borrow to "view" the keys
    let shards_view = shard_mapping.clone();

    // assign data to shards, sorted in, so stored sorted
    hashed_names.sort_by(|a,b| a.total_cmp(&b));
    for hashed_name in hashed_names {
        let mut assign_to: &ShardInfo = &shards_view.
            first_key_value().
            expect("shard_mapping must be populated").0;
        for (shard_info, _data) in shards_view.iter() {
            // as soon as you find next largest value
            // correct shard is counter-clockwise (-1)
            if shard_info.shard_key > hashed_name {
                break;
            }
            assign_to = shard_info;
        }
        let data = shard_mapping.get_mut(&assign_to).
            expect("key must be present");
        data.push(hashed_name);
    }
    println!("Finished sharding:\n{:#?}", shard_mapping);

    // goal: 15 shards -> 30 shards
    // can't assume uniform distribution for small nums
    println!("Time to reshard!");

    // add shards
    for shard_no in 15..30 {
        let shard_name = format!("shard_{}", shard_no);
        let shard_hash = consistent_hash(calculate_hash(&shard_name));
        let shard_info = ShardInfo {
            shard_name: shard_name.clone(),
            shard_key: shard_hash
        };
        // just for convenience
        shards.insert(shard_name.clone(), shard_info.clone());
        // the actual data holder
        let data: Vec<f64> = Vec::new();
        shard_mapping.insert(shard_info, data);
    }

    // updated to include the new empty shards
    let shards_view = shard_mapping.clone();
    // counter to see how many pieces of data move shards
    let mut moves = 0;

    // each shard must be checked for moves
    for (origin_shard_info, hashed_names) in shards_view.iter() {
        for hashed_name in hashed_names {
            let mut assign_to: &ShardInfo = &shards_view.
                first_key_value().
                expect("shard_mapping must be populated").0;
            for (shard_info, _data) in shards_view.iter() {
                // as soon as you find next largest value
                // correct shard is counter-clockwise (-1)
                if shard_info.shard_key > *hashed_name {
                    break;
                }
                assign_to = shard_info;
            }
            // don't move to your own shard unnecessarily
            if assign_to != origin_shard_info {
                moves += 1;

                // copy the data to the new shard
                let data = shard_mapping.get_mut(&assign_to).
                    expect("key must be present");
                data.push(*hashed_name);

                // remove the data from the old shard
                let leaving = origin_shard_info;
                let data = shard_mapping.get_mut(&leaving).
                    expect("key must be present");
                // FIXME: this is awful, I know, but
                // for this iteration simpler to leave
                // the shard data as a Vec
                let mut index_to_remove = 0;
                for e in data.iter() {
                    if e.total_cmp(&hashed_name) == Ordering::Equal {
                        break;
                    } else {
                        index_to_remove += 1;
                    }
                }
                data.remove(index_to_remove);
            }
        }
    }

    println!("Finished resharding:\n{:#?}", shard_mapping);
    println!("Times data moved: {}", moves);
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
