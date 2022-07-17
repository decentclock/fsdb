use rmp_serde::{decode, encode};
use std::fmt::Debug;
use std::fs;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

extern crate serde;

use serde::{de::DeserializeOwned, Serialize};

pub struct Fsdb {
    dir: PathBuf,
}

pub struct Bucket<V> {
    dir: PathBuf,
    _v: PhantomData<V>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("dncode error: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
}

type Result<T> = std::result::Result<T, Error>;

impl Fsdb {
    /// Create a new Fsdb
    pub fn new(dir: &str) -> Result<Self> {
        if !Path::new(dir).exists() {
            fs::create_dir_all(dir)?;
        }
        Ok(Self { dir: dir.into() })
    }

    // Create new bucket
    pub fn bucket<V: Serialize + DeserializeOwned>(&self, p: &str) -> Result<Bucket<V>> {
        let mut dir = self.dir.clone();
        dir.push::<PathBuf>(p.into());
        if !Path::new(&dir).exists() {
            fs::create_dir(dir.clone())?;
        }
        Ok(Bucket {
            dir: dir.into(),
            _v: PhantomData,
        })
    }
}

impl<V: Serialize + DeserializeOwned> Bucket<V> {
    /// Check if a key exists
    pub fn exists(&self, key: &str) -> bool {
        let mut path = self.dir.clone();
        path.push(key);
        path.exists()
    }
    /// Create a key
    pub fn put(&self, key: &str, value: V) -> Result<()> {
        let mut path = self.dir.clone();
        path.push::<PathBuf>(key.into());
        let mut f = fs::File::create(path.clone())?;
        encode::write(&mut f, &value)?;
        Ok(())
    }
    /// Get a key
    pub fn get(&self, key: &str) -> Result<V> {
        let mut path = self.dir.clone();
        path.push(key);
        let f = fs::File::open(path)?;
        Ok(decode::from_read(f)?)
    }
    /// Delete a file
    pub fn remove(&self, key: &str) -> Result<()> {
        let mut path = self.dir.clone();
        path.push::<PathBuf>(key.into());
        Ok(std::fs::remove_file(path)?)
    }
    /// List keys in this bucket
    pub fn list(&self) -> Result<Vec<String>> {
        let path = self.dir.clone();
        let paths = fs::read_dir(path)?;
        let mut r = Vec::new();
        paths.for_each(|name| {
            if let Ok(na) = name {
                if let Ok(n) = na.file_name().into_string() {
                    r.push(n);
                }
            }
        });
        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use crate::Fsdb;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    struct Thing {
        n: u8,
    }

    #[test]
    fn test_db() {
        let db = Fsdb::new("testdb").expect("fail Fsdb::new");
        let b = db.bucket("hi").expect("fail bucket");
        let t1 = Thing { n: 1 };
        b.put("key", t1.clone()).expect("failed to save");
        let t2: Thing = b.get("key").expect("fail to load");
        println!("t {:?}", t2.clone());
        assert_eq!(t1, t2);
        let list = b.list().expect("fail list");
        assert_eq!(list, vec!["key".to_string()]);
    }
}
