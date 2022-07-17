# fsdb

Filesystem database

### usage:

```rust
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Thing {
    n: u8,
}

fn main() -> Result<()> {
    let db = Fsdb::new("testdb")?;
    let b = db.bucket("testbucket")?;

    let t1 = Thing { n: 1 };
    b.put("testkey", t1.clone())?

    let t2: Thing = b.get("testkey")?
    assert_eq!(t1, t2);
    Ok(())
}
```