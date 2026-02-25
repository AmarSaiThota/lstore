"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        self.table = table
        self.indices = [None] * table.num_columns
        self.active = [False] * table.num_columns
        self.create_index(table.key)

    def is_active(self, column):
        return self.active[column]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.active[column] and self.indices[column] is not None:
            return list(self.indices[column].get(value, set()))
        out = []
        for rid, pr_id, bp_id, idx in self.table._iter_base_records():
            pr = self.table.page_ranges[pr_id]
            ind = pr.base_page_sets[bp_id].pages[self.table.indirection_column].read(idx)
            if ind == self.table.DELETED_INDIRECTION:
                continue
            vals = self.table.read_latest_record(rid)
            if vals is None:
                continue
            if vals[column] == value:
                out.append(rid)
        return out

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if self.active[column] and self.indices[column] is not None:
            out = []
            d = self.indices[column]
            for v, rset in d.items():
                if begin <= v <= end:
                    out.extend(rset)
            return out
        out = []
        for rid, pr_id, bp_id, idx in self.table._iter_base_records():
            pr = self.table.page_ranges[pr_id]
            ind = pr.base_page_sets[bp_id].pages[self.table.indirection_column].read(idx)
            if ind == self.table.DELETED_INDIRECTION:
                continue
            vals = self.table.read_latest_record(rid)
            if vals is None:
                continue
            v = vals[column]
            if begin <= v <= end:
                out.append(rid)
        return out

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.active[column_number]:
            return
        self.indices[column_number] = {}
        self.active[column_number] = True
        for rid, pr_id, bp_id, idx in self.table._iter_base_records():
            pr = self.table.page_ranges[pr_id]
            ind = pr.base_page_sets[bp_id].pages[self.table.indirection_column].read(idx)
            if ind == self.table.DELETED_INDIRECTION:
                continue
            vals = self.table.read_latest_record(rid)
            if vals is None:
                continue
            self.add(column_number, vals[column_number], rid)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
        self.active[column_number] = False

    def add(self, column, value, base_rid):
        if not self.active[column]:
            return
        d = self.indices[column]
        if d is None:
            return
        if value not in d:
            d[value] = set()
        d[value].add(base_rid)

    def remove(self, column, value, base_rid):
        if not self.active[column]:
            return
        d = self.indices[column]
        if d is None:
            return
        if value in d:
            d[value].discard(base_rid)
            if not d[value]:
                del d[value]
