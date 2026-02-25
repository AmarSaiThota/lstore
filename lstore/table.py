from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
TPS_COLUMN = 4
BASE_RID_COLUMN = 5
NUM_META_COLUMNS = 6
MAX_BASE_PAGES_PER_RANGE = 16
PAGE_SIZE = 4096
RECORDS_PER_PAGE = 512
NULL_VALUE = -(2**63)
DELETED_INDIRECTION = -2


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.NULL_VALUE = NULL_VALUE
        self.meta_columns = NUM_META_COLUMNS
        self.indirection_column = INDIRECTION_COLUMN
        self.schema_column = SCHEMA_ENCODING_COLUMN
        self.DELETED_INDIRECTION = DELETED_INDIRECTION
        self.page_directory = {}
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.bufferpool = None
        self.key_to_rid = {}
        self.sorted_keys = []
        self.total_columns = NUM_META_COLUMNS + num_columns
        self.page_ranges = [self._new_page_range()]
        self.next_rid = 1
        self.index = Index(self)

    def _new_page_range(self):
        return PageRange(self.num_columns)

    def _new_base_page(self):
        return BasePage(self.num_columns)

    def _new_tail_page(self):
        return TailPage(self.num_columns)

    def _iter_base_records(self):
        for pr_id, pr in enumerate(self.page_ranges):
            for bp_id, bp in enumerate(pr.base_page_sets):
                n = bp.pages[0].num_records
                for idx in range(n):
                    rid = bp.pages[RID_COLUMN].read(idx)
                    if rid == 0:
                        continue
                    yield rid, pr_id, bp_id, idx

    def addPageRange(self):
        if self.page_ranges[-1].is_full():
            self.page_ranges.append(self._new_page_range())

    def insert(self, *columns):
        if len(columns) != self.num_columns:
            return False
        pk = columns[self.key]
        if self.index.locate(self.key, pk):
            return False
        self.addPageRange()
        rid = self.next_rid
        self.next_rid += 1
        indirection = -1
        timestamp = int(time())
        schema_encoding = 0
        tps = 0
        base_rid = rid
        values = [indirection, rid, timestamp, schema_encoding, tps, base_rid] + list(columns)
        page_range_id = len(self.page_ranges) - 1
        base_page_id, idx = self.page_ranges[page_range_id].append_base_record(values)
        self.page_directory[rid] = (page_range_id, 0, base_page_id, idx)
        for col, v in enumerate(columns):
            if self.index.is_active(col):
                self.index.add(col, v, rid)
        return rid

    def TailAppend(self, base_rid, *columns):
        if base_rid not in self.page_directory:
            return False
        page_range_id, is_tail, base_page_id, base_idx = self.page_directory[base_rid]
        if is_tail != 0:
            return False
        page_range = self.page_ranges[page_range_id]
        indirection_rid = page_range.read_base_value(base_page_id, INDIRECTION_COLUMN, base_idx)
        if indirection_rid == DELETED_INDIRECTION:
            return False
        prev_version_rid = base_rid if indirection_rid == -1 else indirection_rid
        tail_rid = self.next_rid
        self.next_rid += 1
        if len(columns) != self.num_columns:
            return False
        if columns[self.key] != self.NULL_VALUE:
            return False
        bits = [0] * self.num_columns
        for i in range(self.num_columns):
            if columns[i] != self.NULL_VALUE:
                bits[i] = 1
        schema = 0
        for i in range(self.num_columns):
            schema += bits[i] * (2 ** i)
        indirection = prev_version_rid
        timestamp = int(time())
        tps = 0
        tail_values = [indirection, tail_rid, timestamp, schema, tps, base_rid] + list(columns)
        tail_page_id, tail_idx = page_range.append_tail_record(tail_values)
        self.page_directory[tail_rid] = (page_range_id, 1, tail_page_id, tail_idx)
        page_range.base_page_sets[base_page_id].pages[INDIRECTION_COLUMN].update(base_idx, tail_rid)
        old_schema = page_range.base_page_sets[base_page_id].pages[SCHEMA_ENCODING_COLUMN].read(base_idx)
        page_range.base_page_sets[base_page_id].pages[SCHEMA_ENCODING_COLUMN].update(base_idx, old_schema | schema)
        return tail_rid

    def read_latest_record(self, base_rid):
        if base_rid not in self.page_directory:
            return None
        pr_id, is_tail, base_page_id, base_idx = self.page_directory[base_rid]
        if is_tail != 0:
            return None
        pr = self.page_ranges[pr_id]
        ind = pr.base_page_sets[base_page_id].pages[INDIRECTION_COLUMN].read(base_idx)
        if ind == DELETED_INDIRECTION:
            return None
        vals = [0] * self.num_columns
        off = NUM_META_COLUMNS
        for c in range(self.num_columns):
            vals[c] = pr.base_page_sets[base_page_id].pages[off + c].read(base_idx)
        tail_records = []
        cur = ind
        while cur != -1 and cur != DELETED_INDIRECTION:
            pd2 = self.page_directory.get(cur)
            if not pd2:
                break
            pr2_id, is_tail2, pg2, idx2 = pd2
            if is_tail2 == 0:
                break
            tail_records.append((pr2_id, pg2, idx2))
            pr2 = self.page_ranges[pr2_id]
            cur = pr2.tail_page_sets[pg2].pages[INDIRECTION_COLUMN].read(idx2)
        for j in range(len(tail_records) - 1, -1, -1):
            pr2_id, pg2, idx2 = tail_records[j]
            pr2 = self.page_ranges[pr2_id]
            schema = pr2.tail_page_sets[pg2].pages[SCHEMA_ENCODING_COLUMN].read(idx2)
            for c in range(self.num_columns):
                if c == self.key:
                    continue
                if (schema >> c) & 1:
                    vals[c] = pr2.tail_page_sets[pg2].pages[off + c].read(idx2)
        return vals

    def __merge(self):
        for pr_idx, pr in enumerate(self.page_ranges):
            if pr.tail_record_count < self.merge_threshold_pages:
                continue
            cutoff_tail_rid = self._get_latest_tail_rid(pr_idx)
            if cutoff_tail_rid is None:
                continue
            for bps in pr.base_page_sets:
                num_records = bps.pages[0].num_records
                for slot in range(num_records):
                    base_rid = bps.pages[RID_COLUMN].read(slot)
                    if base_rid == 0:
                        continue
                    ind = bps.pages[INDIRECTION_COLUMN].read(slot)
                    if ind == DELETED_INDIRECTION:
                        continue
                    current_tps = bps.pages[TPS_COLUMN].read(slot)
                    merged_vals = [0] * self.num_columns
                    for c in range(self.num_columns):
                        merged_vals[c] = bps.pages[NUM_META_COLUMNS + c].read(slot)
                    tail_records = []
                    cur = ind
                    while cur != -1 and cur != DELETED_INDIRECTION:
                        pd = self.page_directory.get(cur)
                        if not pd:
                            break
                        t_pr_id, t_is_tail, t_pg, t_idx = pd
                        if t_is_tail == 0:
                            break
                        if current_tps != 0 and cur <= current_tps:
                            break
                        if cur <= cutoff_tail_rid:
                            tail_records.append((t_pr_id, t_pg, t_idx))
                        t_pr = self.page_ranges[t_pr_id]
                        cur = t_pr.tail_page_sets[t_pg].pages[INDIRECTION_COLUMN].read(t_idx)
                    for j in range(len(tail_records) - 1, -1, -1):
                        t_pr_id, t_pg, t_idx = tail_records[j]
                        t_pr = self.page_ranges[t_pr_id]
                        schema = t_pr.tail_page_sets[t_pg].pages[SCHEMA_ENCODING_COLUMN].read(t_idx)
                        for c in range(self.num_columns):
                            if c == self.key:
                                continue
                            if (schema >> c) & 1:
                                merged_vals[c] = t_pr.tail_page_sets[t_pg].pages[NUM_META_COLUMNS + c].read(t_idx)
                    for c in range(self.num_columns):
                        if c == self.key:
                            continue
                        bps.pages[NUM_META_COLUMNS + c].update(slot, merged_vals[c])
                    bps.pages[TPS_COLUMN].update(slot, cutoff_tail_rid)

    def _get_latest_tail_rid(self, pr_idx):
        pr = self.page_ranges[pr_idx]
        if not pr.tail_page_sets:
            return None
        last_tps = pr.tail_page_sets[-1]
        num_records = last_tps.pages[0].num_records
        if num_records == 0:
            return None
        return last_tps.pages[RID_COLUMN].read(num_records - 1)

    def rebuild_page_directory(self):
        self.page_directory = {}
        for rid, pr_id, bp_id, idx in self._iter_base_records():
            self.page_directory[rid] = (pr_id, 0, bp_id, idx)
        for pr_id, pr in enumerate(self.page_ranges):
            for tp_id, tp in enumerate(pr.tail_page_sets):
                n = tp.pages[0].num_records
                for idx in range(n):
                    rid = tp.pages[RID_COLUMN].read(idx)
                    if rid == 0:
                        continue
                    self.page_directory[rid] = (pr_id, 1, tp_id, idx)

    def rebuild_indices(self):
        self.index = Index(self)

class BasePage:

    def __init__(self, num_total_columns):
        self.num_total_columns = num_total_columns + NUM_META_COLUMNS
        self.pages = [Page() for _ in range(self.num_total_columns)]

    def write(self, values):
        for col in range(self.num_total_columns):
            self.pages[col].write(values[col])

    def read(self, col, idx):
        return self.pages[col].read(idx)

class TailPage:

    def __init__(self, num_total_columns):
        self.num_total_columns = num_total_columns + NUM_META_COLUMNS
        self.pages = [Page() for _ in range(self.num_total_columns)]

    def write(self, values):
        for col in range(self.num_total_columns):
            self.pages[col].write(values[col])

    def read(self, col, idx):
        return self.pages[col].read(idx)


class PageRange():

    def __init__(self, num_columns):
        self.base_page_sets = []
        self.tail_page_sets = []
        self.num_columns = num_columns
        self.base_record_count = 0
        self.tail_record_count = 0

    def ensure_base(self):
        if not self.base_page_sets or not self.base_page_sets[-1].pages[0].has_capacity():
            self.base_page_sets.append(BasePage(self.num_columns))

    def ensure_tail(self):
        if not self.tail_page_sets or not self.tail_page_sets[-1].pages[0].has_capacity():
            self.tail_page_sets.append(TailPage(self.num_columns))

    def append_base_record(self, values):
        self.ensure_base()
        idx = self.base_page_sets[-1].pages[0].num_records
        self.base_page_sets[-1].write(values)
        self.base_record_count += 1
        return len(self.base_page_sets) - 1, idx

    def append_tail_record(self, values):
        self.ensure_tail()
        idx = self.tail_page_sets[-1].pages[0].num_records
        self.tail_page_sets[-1].write(values)
        self.tail_record_count += 1
        return len(self.tail_page_sets) - 1, idx

    def read_base_value(self, base_page_id, col, idx):
        return self.base_page_sets[base_page_id].read(col, idx)

    def read_tail_value(self, tail_page_id, col, idx):
        return self.tail_page_sets[tail_page_id].read(col, idx)

    def is_full(self):
        if len(self.base_page_sets) < MAX_BASE_PAGES_PER_RANGE:
            return False
        return not self.base_page_sets[-1].pages[0].has_capacity()
