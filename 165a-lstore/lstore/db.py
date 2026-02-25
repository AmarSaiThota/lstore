from lstore.table import Table, PageRange, BasePage, TailPage
from lstore.config import *
import os
import json
import pickle


class BufferPool:
    """
    Real BufferPool with LRU eviction, dirty tracking, and disk I/O.
    Page key format: (table_name, pr_id, 'base'|'tail', page_set_id, col_idx)
    """
    def __init__(self, pool_size=1000):
        self.pool_size = pool_size
        self.pages = {}           # page_key -> Page
        self.dirty = set()        # page_keys that are dirty
        self.pin_counts = {}      # page_key -> int
        self.access_order = []    # LRU list (most recent at end)
        self.db_path = None       # set by Database.open()

    def get_page(self, page_key):
        """Get a page by key. Load from disk or create new if not in pool."""
        if page_key in self.pages:
            self._touch(page_key)
            return self.pages[page_key]
        # Try loading from disk
        page = self._load_from_disk(page_key)
        self._maybe_evict()
        self.pages[page_key] = page
        self.pin_counts[page_key] = 0
        self._touch(page_key)
        return page

    def mark_dirty(self, page_key):
        self.dirty.add(page_key)

    def pin(self, page_key):
        if page_key in self.pin_counts:
            self.pin_counts[page_key] += 1

    def unpin(self, page_key):
        if page_key in self.pin_counts and self.pin_counts[page_key] > 0:
            self.pin_counts[page_key] -= 1

    def flush_all(self):
        """Flush all dirty pages to disk."""
        for page_key in list(self.dirty):
            self._write_to_disk(page_key)
        self.dirty.clear()

    def _touch(self, page_key):
        if page_key in self.access_order:
            self.access_order.remove(page_key)
        self.access_order.append(page_key)

    def _maybe_evict(self):
        while len(self.pages) >= self.pool_size:
            evicted = False
            for key in self.access_order:
                if self.pin_counts.get(key, 0) == 0:
                    # Flush if dirty
                    if key in self.dirty:
                        self._write_to_disk(key)
                        self.dirty.discard(key)
                    # Remove from pool
                    del self.pages[key]
                    del self.pin_counts[key]
                    self.access_order.remove(key)
                    evicted = True
                    break
            if not evicted:
                break  # All pages pinned, can't evict

    def _page_path(self, page_key):
        if not self.db_path:
            return None
        table_name, pr_id, kind, page_set_id, col_idx = page_key
        dir_path = os.path.join(self.db_path, table_name, f"pr_{pr_id}")
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, f"{kind}_{page_set_id}_{col_idx}.bin")

    def _load_from_disk(self, page_key):
        from lstore.page import Page
        path = self._page_path(page_key) if self.db_path else None
        if path and os.path.exists(path):
            page = Page()
            with open(path, 'rb') as f:
                data = f.read()
                page.data = bytearray(data[:PAGE_SIZE])
                page.num_records = int.from_bytes(data[PAGE_SIZE:PAGE_SIZE+4], 'little')
            return page
        return Page()

    def _write_to_disk(self, page_key):
        if not self.db_path:
            return
        path = self._page_path(page_key)
        if path and page_key in self.pages:
            page = self.pages[page_key]
            with open(path, 'wb') as f:
                f.write(bytes(page.data))
                f.write(page.num_records.to_bytes(4, 'little'))


class Database():
    def __init__(self):
        self.tables = []
        self.bufferpool = BufferPool()
        self.db_path = None

    def open(self, path):
        """Open database from disk. Load metadata and reconstruct tables."""
        self.db_path = path
        self.bufferpool.db_path = path
        os.makedirs(path, exist_ok=True)

        meta_path = os.path.join(path, 'meta.pkl')
        if os.path.exists(meta_path):
            with open(meta_path, 'rb') as f:
                meta = pickle.load(f)
            for table_meta in meta['tables']:
                table = Table(table_meta['name'], table_meta['num_columns'], table_meta['key'])
                table.bufferpool = self.bufferpool
                table.next_rid = table_meta['next_rid']
                table.page_directory = table_meta['page_directory']
                table.key_to_rid = table_meta.get('key_to_rid', {})
                table.sorted_keys = table_meta.get('sorted_keys', [])

                # Reconstruct index
                index_data = table_meta.get('index_data', None)
                if index_data:
                    for col_idx, col_dict in enumerate(index_data):
                        for val_str, rid_list in col_dict.items():
                            val = int(val_str)
                            table.index.indices[col_idx][val] = set(rid_list)

                # Reconstruct page ranges structure
                pr_metas = table_meta['page_ranges']
                table.page_ranges = []
                for pr_meta in pr_metas:
                    pr = PageRange(table.num_columns)
                    pr.base_record_count = pr_meta['base_record_count']
                    pr.tail_record_count = pr_meta['tail_record_count']

                    # Reconstruct BasePage sets
                    pr.base_page_sets = []
                    for bps_meta in pr_meta['base_page_sets']:
                        bps = BasePage(table.num_columns)
                        # Load page data from bufferpool (which reads from disk)
                        pr_id = len(table.page_ranges)
                        bps_id = len(pr.base_page_sets)
                        for col in range(bps.num_total_columns):
                            page_key = (table.name, pr_id, 'base', bps_id, col)
                            page = self.bufferpool.get_page(page_key)
                            bps.pages[col] = page
                        pr.base_page_sets.append(bps)

                    # Reconstruct TailPage sets
                    pr.tail_page_sets = []
                    for tps_meta in pr_meta['tail_page_sets']:
                        tps = TailPage(table.num_columns)
                        pr_id = len(table.page_ranges)
                        tps_id = len(pr.tail_page_sets)
                        for col in range(tps.num_total_columns):
                            page_key = (table.name, pr_id, 'tail', tps_id, col)
                            page = self.bufferpool.get_page(page_key)
                            tps.pages[col] = page
                        pr.tail_page_sets.append(tps)

                    table.page_ranges.append(pr)

                self.tables.append(table)

    def close(self):
        """Flush all dirty pages and write metadata to disk."""
        if not self.db_path:
            return

        # First, register all current pages in the bufferpool so they get flushed
        for table in self.tables:
            for pr_id, pr in enumerate(table.page_ranges):
                for bps_id, bps in enumerate(pr.base_page_sets):
                    for col in range(bps.num_total_columns):
                        page_key = (table.name, pr_id, 'base', bps_id, col)
                        if page_key not in self.bufferpool.pages:
                            self.bufferpool.pages[page_key] = bps.pages[col]
                            self.bufferpool.pin_counts[page_key] = 0
                        self.bufferpool.mark_dirty(page_key)
                for tps_id, tps in enumerate(pr.tail_page_sets):
                    for col in range(tps.num_total_columns):
                        page_key = (table.name, pr_id, 'tail', tps_id, col)
                        if page_key not in self.bufferpool.pages:
                            self.bufferpool.pages[page_key] = tps.pages[col]
                            self.bufferpool.pin_counts[page_key] = 0
                        self.bufferpool.mark_dirty(page_key)

        self.bufferpool.flush_all()

        # Write metadata
        meta = {'tables': []}
        for table in self.tables:
            # Serialize index as {col: {str(val): [rids]}}
            index_data = []
            for col_idx in range(table.num_columns):
                col_dict = {}
                for val, rids in table.index.indices[col_idx].items():
                    col_dict[str(val)] = list(rids)
                index_data.append(col_dict)

            pr_metas = []
            for pr in table.page_ranges:
                bps_metas = []
                for bps in pr.base_page_sets:
                    bps_metas.append({'num_records': bps.pages[0].num_records if bps.pages else 0})
                tps_metas = []
                for tps in pr.tail_page_sets:
                    tps_metas.append({'num_records': tps.pages[0].num_records if tps.pages else 0})
                pr_metas.append({
                    'base_record_count': pr.base_record_count,
                    'tail_record_count': pr.tail_record_count,
                    'base_page_sets': bps_metas,
                    'tail_page_sets': tps_metas,
                })

            table_meta = {
                'name': table.name,
                'num_columns': table.num_columns,
                'key': table.key,
                'next_rid': table.next_rid,
                'page_directory': table.page_directory,
                'key_to_rid': table.key_to_rid,
                'sorted_keys': table.sorted_keys,
                'index_data': index_data,
                'page_ranges': pr_metas,
            }
            meta['tables'].append(table_meta)

        meta_path = os.path.join(self.db_path, 'meta.pkl')
        with open(meta_path, 'wb') as f:
            pickle.dump(meta, f)

    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        table.bufferpool = self.bufferpool
        self.tables.append(table)
        return table

    def drop_table(self, name):
        self.tables = [t for t in self.tables if t.name != name]

    def get_table(self, name):
        for t in self.tables:
            if t.name == name:
                return t
        return None