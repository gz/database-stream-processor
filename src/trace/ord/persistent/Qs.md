some questions:

- what is the access pattern of the batches
  - e.g., are all batches accessed at all times?
  - is there a hot-set of batches active at anyt time?
  - Is it fine to just activate/deactive entire batches?
    - <https://github.com/avl/savefile> (e.g., if we use a batch we want to read everything in it)
    - Serde?
  - Or do we need something smarter?
- what is the restriction on K (looks like Ord+some stuff)
- what is the restriction on V (looks like Ord+some stuff)
- How big to the Vectors get?

Let's say we use mmapped/fs-based vectors

- we can make it work with mmap/persisted in files and give same interface as Vec
  - Yes but some issues
    - if we have arbitrary objects need to serialize/deserialize on every access (probably keep some cache)
      - Do we care about supporting different machines (if no we could use abomonation)
    - what's the access pattern, can we assume serial access (then cache might be easy)?
- but how do we ensure crash safety?
  - do we need transactions?
  - a database?
    - what transactional semantics do we need?
    - which database?
      - sled? (limitation keys need to be AsRef<[u8]>) not recommended for production by some
      - rustbreak: The Database needs to fit into memory
      - sqlite
      - bonsaidb
    - how do we represent arbitrary rust types in DB and ensure same semantics for key lookup/join etc. (e.g., we need to preserve Ord semnantics for lookups?)
  - or can we get away with a simpler scheme?
    - If batch/vec is immutable we can either persist it all or none for example (something like write batch, fsync, set initialized flag, fsync)?
    - What do we do if we have some batches but not all of them (for a trace, or for the whole program)?
    - What about all these Vec::new calls they seem to indicate Vecs can grow over time?
