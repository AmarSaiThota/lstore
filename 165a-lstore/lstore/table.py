from lstore.index import Index
from lstore.page import Page
from lstore.config import *
from time import time


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

        self.key_to_rid = {}
        self.sorted_keys = []
        self.page_directory = {}

        self.index = Index(self)

        self.merge_threshold_pages = 50  # The threshold to trigger a merge

        self.total_columns = NUM_META_COLUMNS + num_columns
        self.page_ranges = [PageRange(num_columns)]
        self.next_rid = 1 #-1 for no pointer/delete
    
    def addPageRange(self):
        if self.page_ranges[-1].is_full():
            self.page_ranges.append(PageRange(self.num_columns))

    def insert(self, *columns):
        if len(columns) != self.num_columns:
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
        PageRange_id = len(self.page_ranges) - 1
        base_page_id, idx = self.page_ranges[PageRange_id].append_base_record(values)
        self.page_directory[rid] = (PageRange_id, 0, base_page_id, idx)
        key_value = columns[self.key]
        self.index.add(self.key, key_value, rid)
        for col, v in enumerate(columns):
            self.index.add(col, v, rid)
        return rid

    def TailAppend(self, base_rid, *columns):
        '''
        Append Tail objects to the read only base
        - Does not branch out from other tails, sequentially sets indirection from base to tail, and from tail to previous tail
        '''
        PageRange_id, is_tail, base_page_id, base_idx = self.page_directory[base_rid]  #grab rid info
        if is_tail == 0:      
            PageRange = self.page_ranges[PageRange_id] #PageRange is our PageRange() object
            indirection_rid = PageRange.read_base_value(base_page_id, INDIRECTION_COLUMN, base_idx)
            prev_version_rid = base_rid if indirection_rid == -1 else indirection_rid
            tail_rid = self.next_rid
            self.next_rid += 1
            bits = [0] * self.num_columns
            for i in range(self.num_columns):
                if columns[i] != self.NULL_VALUE:
                    bits[i] = 1
            schema = 0#convert bit array into int
            for i in range(self.num_columns):
                schema += bits[i] * (2 ** i) #turn on ith digit if tail record updates value
            
            indirection = prev_version_rid
            timestamp = int(time())
            tps = 0

            tail_values = [indirection, tail_rid, timestamp, schema, tps, base_rid] + list(columns)

            tail_page_id, tail_idx = PageRange.append_tail_record(tail_values)


            self.page_directory[tail_rid] = (PageRange_id, 1, tail_page_id, tail_idx)

        
            PageRange.base_page_sets[base_page_id].pages[INDIRECTION_COLUMN].update(base_idx, tail_rid)

            old_schema = PageRange.base_page_sets[base_page_id].pages[SCHEMA_COLUMN].read(base_idx)
            PageRange.base_page_sets[base_page_id].pages[SCHEMA_COLUMN].update(base_idx, old_schema | schema)

        return tail_rid



    def __merge(self):
        for pr_idx, pr in enumerate(self.page_ranges):
            if pr.tail_record_count < self.merge_threshold_pages:
                continue

            # Determine cutoff: the latest tail RID currently in this range
            # We merge all tail records up to (and including) this cutoff
            cutoff_tail_rid = self._get_latest_tail_rid(pr_idx)
            if cutoff_tail_rid is None:
                continue

            # For each base record in this page range
            for bps_idx, bps in enumerate(pr.base_page_sets):
                num_records = bps.pages[0].num_records
                for slot in range(num_records):
                    base_rid = bps.pages[RID_COLUMN].read(slot)
                    if base_rid == -1:  # deleted
                        continue
                    current_tps = bps.pages[TPS_COLUMN].read(slot)

                    # Start from base values
                    merged_vals = [0] * self.num_columns
                    for c in range(self.num_columns):
                        merged_vals[c] = bps.pages[NUM_META_COLUMNS + c].read(slot)

                    # Walk tail chain and collect tails up to cutoff (stop at TPS)
                    indirection = bps.pages[INDIRECTION_COLUMN].read(slot)
                    tail_records = []
                    cur = indirection
                    while cur != -1:
                        pd = self.page_directory.get(cur)
                        if not pd:
                            break
                        t_pr_id, t_is_tail, t_pg, t_idx = pd
                        if t_is_tail == 0:
                            break
                        # Stop if we've hit already-merged territory
                        if current_tps != 0 and cur <= current_tps:
                            break
                        # Only merge tails up to cutoff
                        if cur <= cutoff_tail_rid:
                            tail_records.append((t_pr_id, t_pg, t_idx))
                        t_pr = self.page_ranges[t_pr_id]
                        cur = t_pr.tail_page_sets[t_pg].pages[INDIRECTION_COLUMN].read(t_idx)

                    # Apply tail deltas oldest-first
                    for j in range(len(tail_records) - 1, -1, -1):
                        t_pr_id, t_pg, t_idx = tail_records[j]
                        t_pr = self.page_ranges[t_pr_id]
                        schema = t_pr.tail_page_sets[t_pg].pages[SCHEMA_COLUMN].read(t_idx)
                        for c in range(self.num_columns):
                            if (schema >> c) & 1:
                                merged_vals[c] = t_pr.tail_page_sets[t_pg].pages[NUM_META_COLUMNS + c].read(t_idx)

                    # Write merged values back to base pages
                    for c in range(self.num_columns):
                        bps.pages[NUM_META_COLUMNS + c].update(slot, merged_vals[c])

                    # Advance TPS to cutoff
                    bps.pages[TPS_COLUMN].update(slot, cutoff_tail_rid)

    def _get_latest_tail_rid(self, pr_idx):
        """Find the latest (highest) tail RID in a page range."""
        pr = self.page_ranges[pr_idx]
        if not pr.tail_page_sets:
            return None
        last_tps = pr.tail_page_sets[-1]
        num_records = last_tps.pages[0].num_records
        if num_records == 0:
            return None
        return last_tps.pages[RID_COLUMN].read(num_records - 1)
 
class BasePage:
    '''
    Capacity filled Logic will be added with PageRange
    '''
    def __init__(self, num_total_columns):
        self.num_total_columns = num_total_columns + NUM_META_COLUMNS
        self.pages = [Page() for i in range(self.num_total_columns)]
    def write(self, values):
        for col in range(self.num_total_columns):
            self.pages[col].write(values[col])
    def read(self, col, idx):
        return self.pages[col].read(idx)
    
class TailPage:
    def __init__(self, num_total_columns):
        self.num_total_columns = num_total_columns + NUM_META_COLUMNS
        self.pages = [Page() for i in range(self.num_total_columns)]
    def write(self, values):
        for col in range(self.num_total_columns):
            self.pages[col].write(values[col])
    def read(self, col, idx):
        return self.pages[col].read(idx)
        


class PageRange():
    '''
    PageRange class dynamically manages base and tail pages. 
    Creates space for extra records by appending new base and tail pages.
    '''
    def __init__(self,num_columns):
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
        
    def append_base_record(self,values):
        self.ensure_base()
        idx = self.base_page_sets[-1].pages[0].num_records
        self.base_page_sets[-1].write(values)
        self.base_record_count += 1
        return len(self.base_page_sets)-1,idx #return location for Table to keep track
    def append_tail_record(self,values):
        self.ensure_tail()
        idx = self.tail_page_sets[-1].pages[0].num_records
        self.tail_page_sets[-1].write(values)
        self.tail_record_count += 1
        return len(self.tail_page_sets)-1,idx #return location for Table to keep track

    def read_base_value(self, base_page_id, col, idx):
        return self.base_page_sets[base_page_id].read(col, idx)

    def read_tail_value(self, tail_page_id, col, idx):
        return self.tail_page_sets[tail_page_id].read(col, idx)
    def is_full(self):
        if len(self.base_page_sets) < 16:
            return False
        return not self.base_page_sets[-1].pages[0].has_capacity()
    





    
    

    
