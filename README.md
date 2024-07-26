# NoSQL database

Data structures and algorithms project by [Ilija](https://github.com/idzaaa9) and [citvaa](https://github.com/citvaa).

Uses `key:value` pairs where key is a string and value could be a string, or could be an array of bytes.
Also supports working with probabilistic data structures serialized as `key:value` pairs.

## Stuff to implement

The whole project should be **fully configurable**. Therefore the 
- [ ] configuration

should be updated as we implement the features.

- [ ] config handling
- [ ] probabilistic data structures
  - [ ] bloom filter
  - [ ] countMinSketch
  - [ ] hyperLogLog
  - [ ] simHash
- [ ] write ahead log
- [ ] memtable
  - memtable should be implemented as a:
  - [ ] hash map
  - [ ] skip list
  - [ ] b-tree
- [ ] SSTable
  - [ ] data part
  - [ ] index part
  - [ ] summary part
  - [ ] bloom filter
  - [ ] metadata(merkle tree)
- [ ] LSM-tree
  - [ ] size-tiered compaction
  - [ ] leveled compaction
- [ ] Cache
  - [ ] LRU
- [ ] Token bucket

