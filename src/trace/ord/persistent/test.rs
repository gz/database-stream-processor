#[test]
fn test_ordered_leaf_simple() {
    use crate::trace::layers::ordered_leaf::{OrderedLeaf, OrderedLeafBuilder, OrderedLeafCursor};
    use crate::trace::layers::Cursor;
    use crate::trace::layers::Trie;
    use crate::trace::layers::{Builder, MergeBuilder, TupleBuilder};

    let _r = env_logger::try_init();

    let mut builder = OrderedLeafBuilder::with_key_capacity(12);
    builder.push_tuple((0usize, 3usize));
    builder.push_tuple((1, 3));
    builder.push_tuple((2, 3));

    let ordered_leaf = builder.done();
    let mut cursor = ordered_leaf.cursor();

    cursor.seek(&ordered_leaf, &(1, 3));

    assert!(cursor.valid(&ordered_leaf));
    assert_eq!(*cursor.key(&ordered_leaf), (1, 3));

    log::info!("{:?}", ordered_leaf);
}

#[test]
fn test_zset_simple() {
    use crate::trace::ord::zset_batch::OrdZSetBuilder;
    use crate::trace::BatchReader;
    use crate::trace::Builder;
    use crate::trace::Cursor;

    let mut builder: OrdZSetBuilder<usize, usize> = OrdZSetBuilder::with_capacity((), 10);
    builder.push((1, (), 2));
    let mut zset = builder.done();

    assert_eq!(zset.len(), 1);
    assert!(!zset.is_empty());

    let mut cursor = zset.cursor();
    assert_eq!(cursor.key_valid(&zset), true);
    assert_eq!(*cursor.key(&zset), 1);
    assert_eq!(cursor.val_valid(&zset), true);
    assert_eq!(cursor.val(&zset), &());
    assert_eq!(cursor.weight(&mut zset), 2);

    cursor.step_key(&zset);
    assert_eq!(cursor.val(&zset), &());
    cursor.step_val(&zset);

    assert_eq!(cursor.key_valid(&zset), false);
    assert_eq!(cursor.val_valid(&zset), false);
}

#[test]
fn test_mtbl_readme() -> std::io::Result<()> {
    // Create a database, using a Sorter instead of a Writer so we can add
    // keys in arbitrary (non-sorted) order.
    {
        use mtbl::{Sorter, Write};
        let mut writer = Sorter::create_from_path("/tmp/f.mtbl", |_k, _v0, _v1| {
            "collision".as_bytes().to_vec()
        })?;
        writer.add("key", "value").expect("added key");
        // Data is flushed to file when the writer/sorter is destroyed.
    }

    use mtbl::Read;
    let reader = mtbl::Reader::open_from_path("/tmp/f.mtbl")?;
    // Get one element
    let val = reader.get("key").unwrap();
    assert_eq!(val, "value".as_bytes());
    // Or iterate over all entries
    for (key, value) in &reader {
        println!("key {:?} value {:?}", key, value);
    }

    Ok(())
}
