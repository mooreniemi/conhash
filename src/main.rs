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
    let mut shards = HashMap::new();
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

    // assign data to shards
    hashed_names.sort_by(|a,b| a.total_cmp(&b));
    for hashed_name in hashed_names {
        println!("{:?}", hashed_name);
        let mut assign_to: &ShardInfo = &shard_mapping.first_key_value().expect("shard_mapping must be populated").0;
        for (shard_info, _data) in shard_mapping.iter() {
            // as soon as you find next largest value
            // correct shard is counter-clockwise (-1)
            if shard_info.shard_key > hashed_name {
                break;
            }
            assign_to = shard_info;
        }
        println!("{:?}", assign_to);
        //let data = shard_mapping.get_mut(&assign_to).expect("key must be present");
        //data.push(hashed_name);
    }

    // goal: 5 shards -> 10 shards
    // 20 names per shard -> 40 names per shard
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
