class Index:
    def __init__(self, table):
        self.table = table
        # indices[c][value] = set({base_rid, base_rid, ...})
        self.indices = [dict() for _ in range(table.num_columns)]

    def add(self, column, value, base_rid):
        d = self.indices[column]
        if value not in d:
            d[value] = set()
        d[value].add(base_rid)

    def remove(self, column, value, base_rid):
        d = self.indices[column]
        if value in d:
            d[value].discard(base_rid)
            if not d[value]:
                del d[value]

    def locate(self, column, value):
        return list(self.indices[column].get(value, set()))

    def locate_range(self, begin, end, column):
        # simple (scan) but correct for M1
        out = []
        d = self.indices[column]
        for v, rset in d.items():
            if begin <= v <= end:
                out.extend(rset)
        return out
