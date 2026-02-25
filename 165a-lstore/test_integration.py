"""
L-Store Integration Test Suite
One-command test that validates: insert, select, update, index, merge,
persistence (close/reopen), bufferpool, sum, versioned queries, and delete.
"""
import os
import shutil
import sys
from random import seed, randint, choice

# Test configuration
DB_PATH = './test_ECS165_integration'
NUM_RECORDS = 500
seed(12345)


def test_insert_select():
    """Basic insert + select correctness."""
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    db.open(DB_PATH)
    t = db.create_table('Test1', 5, 0)
    q = Query(t)

    records = {}
    for i in range(NUM_RECORDS):
        key = 1000 + i
        vals = [key, randint(0, 100), randint(0, 100), randint(0, 100), randint(0, 100)]
        records[key] = vals
        q.insert(*vals)

    errors = 0
    for key, expected in records.items():
        result = q.select(key, 0, [1, 1, 1, 1, 1])
        if not result:
            errors += 1
            continue
        for i, col in enumerate(result[0].columns):
            if col != expected[i]:
                errors += 1
                break

    db.close()
    assert errors == 0, f"{errors} select errors after insert"


def test_update_select():
    """Sparse updates + verify select returns updated values."""
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    db.open(DB_PATH)
    t = db.create_table('Test2', 5, 0)
    q = Query(t)

    records = {}
    for i in range(NUM_RECORDS):
        key = 2000 + i
        vals = [key, randint(0, 100), randint(0, 100), randint(0, 100), randint(0, 100)]
        records[key] = vals
        q.insert(*vals)

    # Sparse updates: only update some columns
    for key in records:
        col_to_update = randint(1, 4)
        new_val = randint(0, 100)
        update_args = [None] * 5
        update_args[col_to_update] = new_val
        q.update(key, *update_args)
        records[key][col_to_update] = new_val

    errors = 0
    for key, expected in records.items():
        result = q.select(key, 0, [1, 1, 1, 1, 1])
        if not result:
            errors += 1
            continue
        for i, col in enumerate(result[0].columns):
            if col != expected[i]:
                errors += 1
                break

    db.close()
    assert errors == 0, f"{errors} select errors after update"


def test_index_after_update():
    """Verify index locate returns correct RIDs after update."""
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    db.open(DB_PATH)
    t = db.create_table('Test3', 3, 0)
    q = Query(t)

    q.insert(100, 10, 20)
    q.insert(200, 30, 40)

    # Update col 1 of key 100 from 10 to 50
    q.update(100, None, 50, None)

    # Old value 10 should not locate key 100
    old_results = t.index.locate(1, 10)
    rids_for_100 = t.index.locate(0, 100)
    assert not any(rid in old_results for rid in rids_for_100), \
        "Index still maps old value after update"

    # New value 50 should locate key 100
    new_results = t.index.locate(1, 50)
    assert any(rid in new_results for rid in rids_for_100), \
        "Index doesn't map new value after update"

    db.close()


def test_merge_correctness():
    """Force merge, verify selects unchanged and TPS advances."""
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    db.open(DB_PATH)
    t = db.create_table('Test4', 3, 0)
    t.merge_threshold_pages = 5  # low threshold
    q = Query(t)

    for i in range(100):
        q.insert(i, i * 10, i * 100)

    # Many updates
    for _ in range(10):
        for i in range(100):
            q.update(i, None, randint(0, 999), None)

    # Record pre-merge values
    pre_merge = {}
    for i in range(100):
        r = q.select(i, 0, [1, 1, 1])
        if r:
            pre_merge[i] = r[0].columns

    # Force merge
    t._Table__merge()

    # Verify selects unchanged
    errors = 0
    for i in range(100):
        r = q.select(i, 0, [1, 1, 1])
        if r:
            if r[0].columns != pre_merge[i]:
                errors += 1

    db.close()
    assert errors == 0, f"{errors} select errors after merge"


def test_persistence():
    """Close + reopen + verify data survives."""
    from lstore.db import Database
    from lstore.query import Query

    # Phase 1: Insert and close
    db = Database()
    db.open(DB_PATH)
    t = db.create_table('Test5', 5, 0)
    q = Query(t)

    records = {}
    for i in range(200):
        key = 5000 + i
        vals = [key, randint(0, 50), randint(0, 50), randint(0, 50), randint(0, 50)]
        records[key] = vals
        q.insert(*vals)

    # Update some
    for key in list(records.keys())[:50]:
        new_val = randint(0, 50)
        q.update(key, None, None, new_val, None, None)
        records[key][2] = new_val

    db.close()

    # Phase 2: Reopen and verify
    db2 = Database()
    db2.open(DB_PATH)
    t2 = db2.get_table('Test5')
    assert t2 is not None, "get_table returned None after reopen"
    q2 = Query(t2)

    errors = 0
    for key, expected in records.items():
        result = q2.select(key, 0, [1, 1, 1, 1, 1])
        if not result:
            errors += 1
            continue
        for i, col in enumerate(result[0].columns):
            if col != expected[i]:
                errors += 1
                break

    db2.close()
    assert errors == 0, f"{errors} select errors after reopen"


def test_sum_and_version():
    """Test sum and versioned select."""
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    db.open(DB_PATH)
    t = db.create_table('Test6', 3, 0)
    q = Query(t)

    # Insert with known values
    for i in range(10):
        q.insert(i, i * 10, 0)

    # Sum column 1 (keys 0-9): should be 0+10+20+...+90 = 450
    result = q.sum(0, 9, 1)
    assert result == 450, f"Sum expected 450, got {result}"

    # Update all of column 1
    for i in range(10):
        q.update(i, None, 100, None)

    # Version 0 (latest) sum should be 10 * 100 = 1000
    result_v0 = q.sum_version(0, 9, 1, 0)
    assert result_v0 == 1000, f"Sum version 0 expected 1000, got {result_v0}"

    # Version -1 (original) sum should be 450
    result_vm1 = q.sum_version(0, 9, 1, -1)
    assert result_vm1 == 450, f"Sum version -1 expected 450, got {result_vm1}"

    db.close()


def test_delete():
    """Delete records and verify they're gone."""
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    db.open(DB_PATH)
    t = db.create_table('Test7', 3, 0)
    q = Query(t)

    for i in range(50):
        q.insert(i, i, i)

    # Delete first 25
    for i in range(25):
        q.delete(i)

    # Verify deleted records are gone from index
    for i in range(25):
        result = q.select(i, 0, [1, 1, 1])
        assert not result, f"Key {i} should be deleted but select returned results"

    # Verify remaining records still work
    for i in range(25, 50):
        result = q.select(i, 0, [1, 1, 1])
        assert result, f"Key {i} should exist but select returned nothing"

    db.close()


def test_m3_transactions():
    """Basic transaction test."""
    from lstore.db import Database
    from lstore.query import Query
    from lstore.transaction import Transaction
    from lstore.transaction_worker import TransactionWorker

    db = Database()
    db.open(DB_PATH)
    t = db.create_table('Test8', 3, 0)
    q = Query(t)

    for i in range(100):
        q.insert(i, i * 10, i * 100)

    # Create transactions
    txn = Transaction()
    txn.add_query(q.select, t, 0, 0, [1, 1, 1])
    txn.add_query(q.update, t, 0, None, 999, None)

    worker = TransactionWorker()
    worker.add_transaction(txn)
    worker.run()
    worker.join()

    # Verify the update was applied
    result = q.select(0, 0, [1, 1, 1])
    assert result, "Select after transaction returned nothing"
    assert result[0].columns[1] == 999, f"Expected 999 after txn update, got {result[0].columns[1]}"

    db.close()


# ─────────── Runner ───────────

ALL_TESTS = [
    test_insert_select,
    test_update_select,
    test_index_after_update,
    test_merge_correctness,
    test_persistence,
    test_sum_and_version,
    test_delete,
    test_m3_transactions,
]

if __name__ == '__main__':
    passed = 0
    failed = 0
    for test in ALL_TESTS:
        shutil.rmtree(DB_PATH, ignore_errors=True)
        print(f"  {test.__name__:.<40s}", end=" ")
        try:
            test()
            print("PASS")
            passed += 1
        except Exception as e:
            print(f"FAIL: {e}")
            failed += 1
    shutil.rmtree(DB_PATH, ignore_errors=True)
    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed out of {passed + failed}")
    if failed:
        sys.exit(1)
