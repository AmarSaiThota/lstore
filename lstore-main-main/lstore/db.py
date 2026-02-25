import json
import os
import struct
from collections import OrderedDict
from lstore.table import Table

class BufferPool:

    def __init__(self, capacity=1000):
        self.capacity = capacity
        self.pages = {}
        self.lru = OrderedDict()
        self.dirty = set()
        self.pins = {}

    def get(self, page_id):
        page = self.pages.get(page_id)
        if page is None:
            return None
        self.lru.pop(page_id, None)
        self.lru[page_id] = True
        return page
    
    def put(self, page_id, page, dirty=False):
        if page_id in self.pages:
            self.pages[page_id] = page
            self.lru.pop(page_id, None)
            self.lru[page_id] = True
            if dirty:
                self.dirty.add(page_id)
            return True
        if len(self.pages) >= self.capacity:
            if not self._evict_one():
                return False
        self.pages[page_id] = page
        self.lru[page_id] = True
        if dirty:
            self.dirty.add(page_id)
        return True

    def mark_dirty(self, page_id):
        if page_id in self.pages:
            self.dirty.add(page_id)

    def pin(self, page_id):
        if page_id in self.pages:
            self.pins[page_id] = self.pins.get(page_id, 0) + 1

    def unpin(self, page_id):
        if page_id in self.pins:
            self.pins[page_id] -= 1
            if self.pins[page_id] <= 0:
                del self.pins[page_id]

    def _evict_one(self):
        for page_id in list(self.lru.keys()):
            if self.pins.get(page_id, 0) != 0:
                continue
            self.lru.pop(page_id, None)
            self.pages.pop(page_id, None)
            self.dirty.discard(page_id)
            return True
        return False


class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = BufferPool()
        self.path = None
        pass

    # Not required for milestone1
    def open(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)
        meta_path = os.path.join(path, "db.meta")
        if not os.path.exists(meta_path):
            return
        with open(meta_path, "r") as f:
            meta = json.load(f)
        self.tables = []
        for tinfo in meta.get("tables", []):
            table = Table(tinfo["name"], tinfo["num_columns"], tinfo["key_index"])
            table.bufferpool = self.bufferpool
            self._load_table(table, os.path.join(path, tinfo["file"]))
            self.tables.append(table)

    def close(self):
        if self.path is None:
            return
        meta = {"tables": []}
        for table in self.tables:
            filename = f"{table.name}.tbl"
            self._save_table(table, os.path.join(self.path, filename))
            meta["tables"].append({
                "name": table.name,
                "num_columns": table.num_columns,
                "key_index": table.key,
                "file": filename,
            })
        meta_path = os.path.join(self.path, "db.meta")
        with open(meta_path, "w") as f:
            json.dump(meta, f)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        table.bufferpool = self.bufferpool
        self.tables.append(table)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i, table in enumerate(self.tables):
            if table.name == name:
                self.tables.pop(i)
                if self.path:
                    filename = os.path.join(self.path, f"{name}.tbl")
                    if os.path.exists(filename):
                        os.remove(filename)
                return True
        return False

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        return None

    def _save_table(self, table, path):
        with open(path, "wb") as f:
            f.write(b"LST2")
            f.write(struct.pack("<I", 1))
            f.write(struct.pack("<I", table.num_columns))
            f.write(struct.pack("<I", table.key))
            f.write(struct.pack("<Q", table.next_rid))
            f.write(struct.pack("<I", len(table.page_ranges)))
            for pr in table.page_ranges:
                f.write(struct.pack("<I", len(pr.base_page_sets)))
                f.write(struct.pack("<I", len(pr.tail_page_sets)))
                f.write(struct.pack("<I", pr.base_record_count))
                f.write(struct.pack("<I", pr.tail_record_count))
                for bp in pr.base_page_sets:
                    for page in bp.pages:
                        f.write(struct.pack("<I", page.num_records))
                        f.write(page.data)
                for tp in pr.tail_page_sets:
                    for page in tp.pages:
                        f.write(struct.pack("<I", page.num_records))
                        f.write(page.data)

    def _load_table(self, table, path):
        if not os.path.exists(path):
            return
        with open(path, "rb") as f:
            magic = f.read(4)
            if magic != b"LST2":
                return
            _version = struct.unpack("<I", f.read(4))[0]
            num_columns = struct.unpack("<I", f.read(4))[0]
            key_index = struct.unpack("<I", f.read(4))[0]
            next_rid = struct.unpack("<Q", f.read(8))[0]
            num_page_ranges = struct.unpack("<I", f.read(4))[0]

            if num_columns != table.num_columns or key_index != table.key:
                return

            table.page_ranges = []
            for _ in range(num_page_ranges):
                base_pages = struct.unpack("<I", f.read(4))[0]
                tail_pages = struct.unpack("<I", f.read(4))[0]
                base_count = struct.unpack("<I", f.read(4))[0]
                tail_count = struct.unpack("<I", f.read(4))[0]
                pr = table._new_page_range()
                pr.base_page_sets = []
                pr.tail_page_sets = []
                pr.base_record_count = base_count
                pr.tail_record_count = tail_count
                for _ in range(base_pages):
                    bp = table._new_base_page()
                    for page in bp.pages:
                        page.num_records = struct.unpack("<I", f.read(4))[0]
                        page.data[:] = f.read(4096)
                    pr.base_page_sets.append(bp)
                for _ in range(tail_pages):
                    tp = table._new_tail_page()
                    for page in tp.pages:
                        page.num_records = struct.unpack("<I", f.read(4))[0]
                        page.data[:] = f.read(4096)
                    pr.tail_page_sets.append(tp)
                table.page_ranges.append(pr)
            table.next_rid = next_rid
            table.rebuild_page_directory()
            table.rebuild_indices()
