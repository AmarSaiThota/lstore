# L-Store: a columnar database engine in Python

A from-scratch implementation of L-Store, a storage architecture that handles fast updates and analytical reads in the same engine. Built as a team project for ECS 165A (Database Systems) at UC Davis, Winter 2026.

## What it does

- **Columnar storage.** Every column lives in its own 4 KB pages of 64-bit values, grouped into page ranges of base and tail pages.
- **Update without overwrite.** Updates append tail records and link them through an indirection column, so older versions stay readable.
- **Versioned queries.** `select_version` and `sum_version` read a record as it was N updates ago.
- **Merge.** Tail records get folded back into base pages, with a tail page sequence number tracking what has been merged.
- **Buffer pool.** Pages load from disk on demand, with LRU eviction, pin counts and dirty-page write-back.
- **Persistence.** `Database.open()` and `close()` save tables and page files to disk and restore them.
- **Indexing.** A primary-key index with point and range lookups.

## Layout

```
165a-lstore/
  lstore/
    config.py              # page size, metadata column layout
    page.py                # fixed-size 64-bit value page
    table.py               # records, base/tail pages, page ranges, merge
    db.py                  # Database and BufferPool
    index.py               # key index with range lookup
    query.py               # insert, select, update, delete, sum (+ versioned)
    transaction.py         # transaction wrapper over queries
    transaction_worker.py  # runs batches of transactions
  m1_tester.py, m2_tester_part*.py, m3_tester_part*.py
  test_integration.py
```

## Run it

```bash
cd 165a-lstore
pip install -r requirements.txt
python m1_tester.py
python m2_tester_part1.py && python m2_tester_part2.py
```

## Tech

Pure Python 3. The engine has no third-party dependencies. Table metadata is stored as JSON and pages as binary files.
