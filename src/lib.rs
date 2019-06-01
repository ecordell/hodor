use std::hash::Hash;
use std::collections::HashMap;
use std::time::{Duration, Instant};

// Cache allows storing values that expire after a given time
// and provides utils (vacuum) for garbage collecting expired keys in the background
trait Cache<K, V> {
    fn insert(&mut self, key : K, value: V) -> Option<V>;
    fn insert_ttl(&mut self, key : K, value: V, ttl: Duration) -> Option<V>;

    // get can remove if expired, so mut here
    fn get(&mut self, key : K) -> Option<&V>;

    fn vacuum(&mut self, count : usize, retry_threshold : f32 );
}

// Value wraps a stored value of type V with (optional) expiration data
struct Value<V> {
    value: V,
    expires: ExpireMeta,
}

// A value is either persistent (never expires) or has expiration metadata attached
enum ExpireMeta {
    Persistent,
    Expires(Expiration)
}

// Expiration is determined based on the instant the value was inserted and the duration it should
// live in the cache
struct Expiration {
    inserted: Instant,
    ttl: Duration,
}

// HashCache is a hashmap-backed cache implementation
pub struct HashCache<K: Hash+Eq+Clone, V> {
    store: HashMap<K,Value<V>>,
    expiring: Vec<K>,
}

impl<K: Hash+Eq+Clone, V>  HashCache<K, V> {
    pub fn new() -> HashCache<K,V> {
        HashCache{ store: HashMap::new(), expiring: Vec::new()}
    }
}

impl<K: Hash+Eq+Clone, V>  Cache<K,V> for HashCache<K, V>  {
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        let inserted = self.store.insert(key, Value{value: value, expires: ExpireMeta::Persistent})?;
        Some(inserted.value)
    }

    fn insert_ttl(&mut self, key: K, value: V, ttl: Duration) -> Option<V> {
        self.expiring.push(key.clone());
        let inserted = self.store.insert(key, Value{value: value, expires:ExpireMeta::Expires(Expiration{inserted: Instant::now(), ttl})})?;
        Some(inserted.value)
    }

    fn get(&mut self, key: K) -> Option<&V> {
        let expired = match self.store.get(&key) {
            Some(v) => {
                match &v.expires {
                    ExpireMeta::Expires(e) => {
                        e.inserted.elapsed().gt(&e.ttl)
                    }
                    _ => {false}
                }
            },
            None => { return None },
        };
        if expired {
            self.store.remove(&key);
            return None
        }

        // entry isn't expired, so fetch and unwrap it
        let v = self.store.get(&key)?;
        Some(&v.value)
    }

    // vacuum samples the set of potentially expired keys and removes them if expired
    // panics if retry-threshold is not between 0 and 1.
    fn vacuum(&mut self, count : usize, retry_threshold : f32 ) {

        // if the ratio of expired keys to sample size > retry threshold,
        // we perform an additional vacuum before exiting
        assert!(retry_threshold > 0.0);
        assert!(retry_threshold < 1.0);

        // amount is the max number of items we sample from the current set
        let mut amount = count;
        if count > self.expiring.len() {
            amount = self.expiring.len()
        }

        // initialize to amount so that we always iterate at least once
        let mut expired_count = amount as f32;

        while expired_count/(amount as f32) > retry_threshold {
            println!("{:?}, {:?}, {:?}", expired_count, amount as f32, retry_threshold);
            expired_count = 0.0;
            if amount > self.expiring.len() {
                amount = self.expiring.len()
            }

            // sample a random set of indices that have expiration set
            let samples = rand::seq::index::sample(&mut rand::thread_rng(), self.expiring.len(), amount);

            // if the key referenced by the index is expired, remove it from the cache (and self.expiring)
            for index in samples.iter() {
                if let Some(key) = self.expiring.get(index) {
                    let expired = match self.store.get(key) {
                        Some(v) => {
                            match &v.expires {
                                ExpireMeta::Expires(e) => {
                                    e.inserted.elapsed().gt(&e.ttl)
                                }
                                _ => { false }
                            }
                        },
                        // if the entry is already gone, mark expired so that the entry in self.expiring is cleared
                        None => { true },
                    };
                    if expired {
                        self.store.remove(key);
                        self.expiring.remove(index);
                        expired_count += 1.0;
                    }
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::{HashCache, Cache};
    use std::time::Duration;
    use std::thread::sleep;

    #[test]
    fn store_retrieve() {
        let mut cache : HashCache<String,String> = HashCache::new();
        cache.insert("id".to_string(), "secret".to_string());

        // todo: there must be some test helpers for this
        if let Some(out) = cache.get("id".to_string()) {
            assert_eq!(*out, "secret".to_string());
        } else {
            panic!("didn't find key in cache")
        }

        if let None = cache.get("nope".to_string()) {
            // all good
        } else {
            panic!("expected none")
        }
    }

    #[test]
    fn expire_key() {
        let mut cache : HashCache<String,String> = HashCache::new();
        cache.insert_ttl("id".to_string(), "secret".to_string(), Duration::new(1, 0));
        assert_eq!(cache.expiring.len(), 1);

        // initial get should work
        if let Some(out) = cache.get("id".to_string()) {
            assert_eq!(*out, "secret".to_string());
        } else {
            panic!("didn't find key in cache")
        }

        sleep(Duration::new(1, 0));

        // fetch after ttl should be none
        if let None = cache.get("id".to_string()) {
            // all good
        } else {
            panic!("expected none")
        }

        // check that it's been removed from the hashmap entirely
        if let None = cache.store.get("id") {
            // all good
        } else {
            panic!("expected store to no longer have key")
        }

        // even though the cache reports the key is gone, it's still tracked in the expiring list
        // until a vacuum is performed
        assert_eq!(cache.expiring.len(), 1);
    }

    #[test]
    fn vacuum() {
        let mut cache : HashCache<String,String> = HashCache::new();
        cache.insert_ttl("id".to_string(), "secret".to_string(), Duration::new(1, 0));
        assert_eq!(cache.expiring.len(), 1);

        // initial get should work
        if let Some(out) = cache.get("id".to_string()) {
            assert_eq!(*out, "secret".to_string());
        } else {
            panic!("didn't find key in cache")
        }

        cache.vacuum(10, 0.25);

        sleep(Duration::new(1, 0));

        cache.vacuum(10, 0.25);

        // check that it's been removed from the hashmap entirely
        // this skips the active removal, so it verifies vacuuming
        if let None = cache.store.get("id") {
            // all good
        } else {
            panic!("expected store to no longer have key")
        }

        // check that key is no longer being tracked as an expiration candiate
        assert_eq!(cache.expiring.len(), 0)
    }

    #[test]
    fn vacuum_sampling_retry() {
        let mut cache : HashCache<String,String> = HashCache::new();
        cache.insert_ttl("id".to_string(), "secret".to_string(), Duration::new(1, 0));
        cache.insert_ttl("id2".to_string(), "secret2".to_string(), Duration::new(1, 0));
        assert_eq!(cache.expiring.len(), 2);

        // wait for keys to expire
        sleep(Duration::new(1, 0));

        // count is 1, but there are two entries, so the retry threshold should be hit (0.5>0.25)
        // and should clean up both entries
        cache.vacuum(1, 0.25);

        // check that it's been removed from the hashmap entirely
        // this skips the active removal, so it verifies vacuuming
        if let None = cache.store.get("id") {
            // all good
        } else {
            panic!("expected store to no longer have key")
        }

        // check that key is no longer being tracked as an expiration candiate
        assert_eq!(cache.expiring.len(), 0)
    }

    #[test]
    fn vacuum_sampling_no_retry() {
        let mut cache : HashCache<String,String> = HashCache::new();
        cache.insert_ttl("id".to_string(), "secret".to_string(), Duration::new(1, 0));
        cache.insert_ttl("id2".to_string(), "secret2".to_string(), Duration::new(1, 0));
        cache.insert_ttl("id3".to_string(), "secret".to_string(), Duration::new(2, 0));
        cache.insert_ttl("id4".to_string(), "secret2".to_string(), Duration::new(2, 0));
        assert_eq!(cache.expiring.len(), 4);

        // wait for 2 keys to expire
        sleep(Duration::new(1, 0));

        // count is 1 and retry threshold is 0.60, so one iteration of vacuuming should leave
        // one remaining entry to clean (0.60 < 0.50)
        cache.vacuum(2, 0.60);

        // check that at least one key was vacuumed
        assert!(cache.expiring.len() < 4);
        // check that no more than two keys were vacuumed
        assert!(cache.expiring.len() >= 2);

    }
}
