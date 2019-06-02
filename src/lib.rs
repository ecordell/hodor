use std::hash::Hash;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::sync::RwLock;

// Cache allows storing values that expire after a given time
// and provides utils (vacuum) for garbage collecting expired keys in the background
trait Cache<K, V> {
    fn insert(&mut self, key : K, value: V) -> Option<V>;
    fn insert_ttl(&mut self, key : K, value: V, ttl: Duration) -> Option<V>;
    fn get<F>(&self, key: K, f: F) -> bool where F: Fn(&V);
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

    fn expired(&self, key: &K) -> bool {
        match self.store.get(key) {
            Some(v) => {
                match &v.expires {
                    ExpireMeta::Expires(e) => {
                        e.inserted.elapsed().gt(&e.ttl)
                    }
                    _ => { false }
                }
            },
            // report empty entries as expired
            None => { true },
        }
    }

    // called by vacuum, this just handles sampling and removing a single set (not retrying based
    // on a threshold)
    fn vacuum_sample(&mut self, count : usize) -> usize {
        // amount is the max number of items we sample from the current set
        let mut amount = count;
        if count > self.expiring.len() {
            amount = self.expiring.len()
        }

        // sample a random set of indices that have expiration set
        let samples = rand::seq::index::sample(&mut rand::thread_rng(), self.expiring.len(), amount);

        let mut expired_indices = vec![];

        // if the key referenced by the index is expired, remove it from the cache (and self.expiring)
        for index in samples.iter() {
            if let Some(key) = self.expiring.get(index) {
                if self.expired(&key) {
                    self.store.remove(key);
                    expired_indices.push(index);
                }
            }
        }

        return expired_indices.iter().map(|i| self.expiring.remove(*i)).count();
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

    fn get<F>(&self, key: K, f: F) -> bool where F: Fn(&V) {
        if self.expired(&key) {
            return false
        }

        // entry isn't expired, so fetch and unwrap it
        if let Some(v) = self.store.get(&key) {
            f(&v.value);
            return true
        }
        false
    }

    // vacuum samples the set of potentially expired keys and removes them if expired
    // panics if retry-threshold is not between 0 and 1.
    fn vacuum(&mut self, count : usize, retry_threshold : f32 ) {

        // if the ratio of expired keys to sample size > retry threshold,
        // we perform an additional vacuum before exiting
        assert!(retry_threshold > 0.0);
        assert!(retry_threshold < 1.0);

        // initialize to amount so that we always iterate at least once
        let mut expired_count = count as f32;

        while expired_count/(count as f32) > retry_threshold {
            expired_count = self.vacuum_sample(count) as f32;
        }
    }
}

pub struct ThreadSafeHashCache<K: Hash+Eq+Clone, V> {
    store: RwLock<HashMap<K,Value<V>>>,
    expiring: RwLock<Vec<K>>,
}

impl<K: Hash+Eq+Clone, V>  ThreadSafeHashCache<K, V> {
    pub fn new() -> ThreadSafeHashCache<K,V> {
        ThreadSafeHashCache{ store: RwLock::new(HashMap::new()), expiring: RwLock::new(Vec::new())}
    }

    fn expired(&self, key: &K) -> bool {
        let c = self.store.read().expect("lock poisoned");

        match c.get(key) {
            Some(v) => {
                match &v.expires {
                    ExpireMeta::Expires(e) => {
                        e.inserted.elapsed().gt(&e.ttl)
                    }
                    _ => { false }
                }
            },
            // report empty entries as expired
            None => { true },
        }
    }

    // called by vacuum, this just handles sampling and removing a single set (not retrying based
    // on a threshold)
    fn vacuum_sample(&mut self, count : usize) -> usize {
        // amount is the max number of items we sample from the current set
        let mut amount = count;
        let expire_len;
        {
            let e = self.expiring.read().expect("lock poisoned");
            expire_len = e.len()
        }

        if count > expire_len {
            amount = expire_len
        }


        // sample a random set of indices that have expiration set
        let samples = rand::seq::index::sample(&mut rand::thread_rng(), expire_len, amount);

        let mut expired_indices = vec![];

        {
            let expiring = self.expiring.read().expect("lock poisoned");
            // if the key referenced by the index is expired, remove it from the cache (and self.expiring)
            for index in samples.iter() {
                if let Some(key) = expiring.get(index) {
                    if self.expired(&key) {
                        let mut store = self.store.write().expect("lock poisoned");
                        store.remove(key);
                        expired_indices.push(index);
                    }
                }
            }
        }

        let mut expiring = self.expiring.write().expect("lock poisoned");
        return expired_indices.iter().map(|i| expiring.remove(*i)).count();
    }
}

impl<K: Hash+Eq+Clone, V>  Cache<K,V> for ThreadSafeHashCache<K, V>  {
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        let mut store = self.store.write().expect("lock poisoned");
        let inserted = store.insert(key, Value{value: value, expires: ExpireMeta::Persistent})?;
        Some(inserted.value)
    }

    fn insert_ttl(&mut self, key: K, value: V, ttl: Duration) -> Option<V> {
        {
            let mut expiring = self.expiring.write().expect("lock poisoned");
            expiring.push(key.clone());
        }
        let mut store = self.store.write().expect("lock poisoned");
        let inserted = store.insert(key, Value { value: value, expires: ExpireMeta::Expires(Expiration { inserted: Instant::now(), ttl }) })?;
        Some(inserted.value)
    }

    fn get<F>(&self, key: K, f: F) -> bool where F: Fn(&V) {
        if self.expired(&key) {
            return false
        }

        // entry isn't expired, so fetch and unwrap it
        if let Some(v) = self.store.read().expect("lock poisoned").get(&key) {
            f(&v.value);
            return true
        }
        false
    }

    // vacuum samples the set of potentially expired keys and removes them if expired
    // panics if retry-threshold is not between 0 and 1.
    fn vacuum(&mut self, count : usize, retry_threshold : f32 ) {
        // if the ratio of expired keys to sample size > retry threshold,
        // we perform an additional vacuum before exiting
        assert!(retry_threshold > 0.0);
        assert!(retry_threshold < 1.0);

        // initialize to amount so that we always iterate at least once
        let mut expired_count = count as f32;

        while expired_count/(count as f32) > retry_threshold {
            expired_count = self.vacuum_sample(count) as f32;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{HashCache, Cache, ThreadSafeHashCache};
    use std::time::Duration;
    use std::thread::{sleep, spawn};
    use std::sync::{Arc, RwLock};

    #[test]
    fn store_retrieve() {
        let mut cache : HashCache<&str,&str> = HashCache::new();
        cache.insert("id", "secret");
        assert_eq!(true,
                   cache.get("id", |v| assert_eq!(*v, "secret")));
        assert_eq!(false,
                   cache.get("nope", |_| panic!("expected none")));
    }

    #[test]
    fn expire_key() {
        let mut cache : HashCache<&str,&str> = HashCache::new();
        cache.insert_ttl("id", "secret", Duration::new(1, 0));
        assert_eq!(cache.expiring.len(), 1);

        // initial get should work
        assert_eq!(true,
                   cache.get("id", |v| assert_eq!(*v, "secret")));

        sleep(Duration::new(1, 0));

        // fetch after ttl should be none
        assert_eq!(false, cache.get("id", |_| panic!("expected none")));

        // even though the cache reports the key is gone, it's still tracked in the expiring list
        // until a vacuum is performed
        assert_eq!(cache.expiring.len(), 1);
        assert_eq!(cache.store.len(), 1);
    }

    #[test]
    fn vacuum() {
        let mut cache : HashCache<&str,&str> = HashCache::new();
        cache.insert_ttl("id", "secret", Duration::new(1, 0));
        assert_eq!(cache.expiring.len(), 1);

        // initial get should work
        assert_eq!(true,
                   cache.get("id", |v| assert_eq!(*v, "secret")));
        cache.vacuum(10, 0.25);

        sleep(Duration::new(1, 0));

        cache.vacuum(10, 0.25);

        // check that it's been removed from the hashmap entirely
        // this skips the active removal, so it verifies vacuuming
        if let Some(_) = cache.store.get("id") {
            panic!("expected store to no longer have key")
        }

        // check that key is no longer being tracked as an expiration candiate
        assert_eq!(cache.expiring.len(), 0)
    }

    #[test]
    fn vacuum_sampling_retry() {
        let mut cache : HashCache<&str,&str> = HashCache::new();
        cache.insert_ttl("id", "secret", Duration::new(1, 0));
        cache.insert_ttl("id2", "secret2", Duration::new(1, 0));
        assert_eq!(cache.expiring.len(), 2);

        // wait for keys to expire
        sleep(Duration::new(1, 0));

        // count is 1, but there are two entries, so the retry threshold should be hit (0.5>0.25)
        // and should clean up both entries
        cache.vacuum(1, 0.25);

        // check that it's been removed from the hashmap entirely
        // this skips the active removal, so it verifies vacuuming
        if let Some(_) = cache.store.get("id") {
            panic!("expected store to no longer have key")
        }

        // check that key is no longer being tracked as an expiration candiate
        assert_eq!(cache.expiring.len(), 0)
    }

    #[test]
    fn vacuum_sampling_no_retry() {
        let mut cache : HashCache<&str,&str> = HashCache::new();
        cache.insert_ttl("id", "secret", Duration::new(1, 0));
        cache.insert_ttl("id2", "secret2", Duration::new(1, 0));
        cache.insert_ttl("id3", "secret", Duration::new(2, 0));
        cache.insert_ttl("id4", "secret2", Duration::new(2, 0));
        assert_eq!(cache.expiring.len(), 4);

        // wait for 2 keys to expire
        sleep(Duration::new(1, 1000));

        // count is 4 and retry threshold is 0.60, so one iteration of vacuuming should leave
        // two entries remaining
        cache.vacuum(4, 0.60);

        // check that two keys were vacuumed
        assert_eq!(2, cache.expiring.len());
    }

    #[test]
    fn threadsafe_cache_e2e() {
        let cache : Arc<RwLock<ThreadSafeHashCache<&str,&str>>> = Arc::new(RwLock::new(ThreadSafeHashCache::new()));
        let vacuum_cache = cache.clone();

        // start a vacuum thread
        spawn(move || {
            loop {
                let mut c = vacuum_cache.write().expect("poisoned lock");
                c.vacuum(10, 0.25);
                sleep(Duration::new(1,0));
            }
        });

        let c = cache.clone();

        // insert a value
        {
            let mut outer = c.write().expect("poisoned lock");
            outer.insert_ttl("id", "secret", Duration::new(1, 0));
        }

        sleep(Duration::new(2,0));

        // check that key was vacuumed
        {
            let outer = c.read().expect("poisoned lock");
            assert_eq!(0, outer.expiring.read().expect("poisoned lock").len());
        }
    }
}
