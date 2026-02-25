from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.NULL_VALUE = NULL_VALUE
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            for rid in rids:
                pd = self.table.page_directory.get(rid)
                if not pd:
                    continue
                pr_id,is_tail,pg,idx = pd
                if is_tail !=0:
                    continue
                pr = self.table.page_ranges[pr_id]#we are trying to detect and call all the indirections 
                pr.base_page_sets[pg].pages[0].update(idx,-1)
                self.table.index.remove(self.table.key,primary_key,rid)
            return True
        except:
            return False
         
        
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        try:
            if len(columns) != self.table.num_columns:
                return False
            primary_key = columns[self.table.key]
            if self.table.index.locate(self.table.key, primary_key):
                return False
            rid = self.table.insert(*columns)
            if not rid:
                return False
            return True
        except:
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return[]
            res = []
            for rid in rids:
                pd = self.table.page_directory.get(rid)
                if not pd:
                    continue
                pr_id, is_tail, pg, idx = pd
                if is_tail !=0:
                    continue
                pr = self.table.page_ranges[pr_id]
                off = NUM_META_COLUMNS
                vals = [0] * self.table.num_columns 
                for c in range(self.table.num_columns):
                    v = pr.base_page_sets[pg].pages[off +c].read(idx)
                    vals[c] = v
                ic = INDIRECTION_COLUMN
                sc = SCHEMA_COLUMN
                trs = []
                cur = pr.base_page_sets[pg].pages[ic].read(idx)
                #over here then we can foillow the indirection chain here even if the yes are tired
                while True:
                    if cur == -1:
                        break
                    pd2 = self.table.page_directory.get(cur)
                    if not pd2:
                        break
                    pr2_id,is_tail2,pg2,idx2 = pd2
                    if is_tail2 == 0:
                        break
                    trs.append((pr2_id, pg2, idx2))
                    pr2 = self.table.page_ranges[pr2_id]
                    cur = pr2.tail_page_sets[pg2].pages[ic].read(idx2)
                for j in range(len(trs)-1,-1,-1):
                    pr2_id,pg2,idx2 = trs[j]
                    pr2 = self.table.page_ranges[pr2_id]
                    s = pr2.tail_page_sets[pg2].pages[sc].read(idx2)
                    for c in range(self.table.num_columns):
                        if (s >> c) & 1:
                            v =pr2.tail_page_sets[pg2].pages[off + c].read(idx2)
                            vals[c] = v
                cols = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        cols.append(vals[i])
                    else:
                        cols.append(self.NULL_VALUE)
                key = vals[self.table.key]
                res.append(Record(rid, key, cols))
            return res
        except:
            return False


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []
            res = []
            for rid in rids:
                pd = self.table.page_directory.get(rid)
                if not pd:
                    continue
                pr_id, is_tail,pg,idx = pd
                if is_tail != 0:
                    continue
                pr = self.table.page_ranges[pr_id]
                off = NUM_META_COLUMNS
                vals = [0]*self.table.num_columns
                for c in range(self.table.num_columns):
                    v = pr.base_page_sets[pg].pages[off+c].read(idx)
                    vals[c] = v
                ic = INDIRECTION_COLUMN
                sc = SCHEMA_COLUMN
                trs = []
                cur = pr.base_page_sets[pg].pages[ic].read(idx)
                while True:
                    if cur == -1:
                        break
                    pd2 = self.table.page_directory.get(cur)
                    if not pd2:
                        break
                    pr2_id,is_tail2,pg2,idx2 = pd2
                    if is_tail2 == 0:
                        break
                    trs.append((pr2_id, pg2, idx2))
                    pr2 = self.table.page_ranges[pr2_id]
                    cur = pr2.tail_page_sets[pg2].pages[ic].read(idx2)
                n = len(trs)
                if relative_version <= 0:
                    use = n + relative_version
                else:
                    use = n - relative_version
                if use < 0:
                    use = 0
                if use > n:
                    use = n
                trs_rev = trs[::-1]
                for j in range(use):
                    pr2_id, pg2, idx2 = trs_rev[j]
                    pr2 = self.table.page_ranges[pr2_id]
                    s = pr2.tail_page_sets[pg2].pages[sc].read(idx2)
                    for c in range(self.table.num_columns):
                        if (s >> c) & 1:
                            v = pr2.tail_page_sets[pg2].pages[off + c].read(idx2)
                            vals[c] = v
                cols = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        cols.append(vals[i])
                    else:
                        cols.append(self.NULL_VALUE)
                key = vals[self.table.key]
                res.append(Record(rid, key, cols))
            return res
        except:
            return False
                        

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            rids = self.table.index.locate(self.table.key,primary_key)
            if not rids:
                return False
            for rid in rids:
                # Reconstruct current values so we can update the index
                pd = self.table.page_directory.get(rid)
                if not pd:
                    continue
                pr_id, is_tail, pg, idx = pd
                if is_tail != 0:
                    continue
                pr = self.table.page_ranges[pr_id]
                off = NUM_META_COLUMNS
                old_vals = [0] * self.table.num_columns
                for c in range(self.table.num_columns):
                    old_vals[c] = pr.base_page_sets[pg].pages[off + c].read(idx)
                # Walk tail chain to get true current values
                ic = INDIRECTION_COLUMN
                sc = SCHEMA_COLUMN
                trs = []
                cur = pr.base_page_sets[pg].pages[ic].read(idx)
                while True:
                    if cur == -1:
                        break
                    pd2 = self.table.page_directory.get(cur)
                    if not pd2:
                        break
                    pr2_id, is_tail2, pg2, idx2 = pd2
                    if is_tail2 == 0:
                        break
                    trs.append((pr2_id, pg2, idx2))
                    pr2 = self.table.page_ranges[pr2_id]
                    cur = pr2.tail_page_sets[pg2].pages[ic].read(idx2)
                for j in range(len(trs) - 1, -1, -1):
                    pr2_id, pg2, idx2 = trs[j]
                    pr2 = self.table.page_ranges[pr2_id]
                    s = pr2.tail_page_sets[pg2].pages[sc].read(idx2)
                    for c in range(self.table.num_columns):
                        if (s >> c) & 1:
                            old_vals[c] = pr2.tail_page_sets[pg2].pages[off + c].read(idx2)

                # Build update columns
                cols = []
                for i in range(self.table.num_columns):
                    if i < len(columns):
                        v = columns[i]
                        if v is None or v == self.NULL_VALUE:
                            cols.append(self.NULL_VALUE)
                        else:
                            cols.append(v)
                    else:
                        cols.append(self.NULL_VALUE)
                self.table.TailAppend(rid, *cols)

                # Update index entries for changed columns
                for i in range(self.table.num_columns):
                    if cols[i] != self.NULL_VALUE:
                        self.table.index.remove(i, old_vals[i], rid)
                        self.table.index.add(i, cols[i], rid)
            return True 
        except: 
            return False
            

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self,start_range,end_range,aggregate_column_index):
        try:
            rids=self.table.index.locate_range(start_range,end_range,self.table.key)
            if not rids:return False
            total=0
            count=0
            for rid in rids:
                pd=self.table.page_directory.get(rid)
                if not pd:continue
                pr_id,is_tail,pg,idx=pd
                if is_tail!=0:continue
                pr=self.table.page_ranges[pr_id]
                off=NUM_META_COLUMNS
                base_vals=[]
                for col in range(self.table.num_columns):
                    base_vals.append(pr.base_page_sets[pg].pages[off+col].read(idx))
                ic=INDIRECTION_COLUMN
                sc=SCHEMA_COLUMN
                tail_chain=[]
                cur=pr.base_page_sets[pg].pages[ic].read(idx)
                while True:
                    if cur==-1:break
                    pd2=self.table.page_directory.get(cur)
                    if not pd2:break
                    pr2_id,is_tail2,pg2,idx2=pd2
                    if is_tail2==0:break
                    tail_chain.append((pr2_id,pg2,idx2))
                    pr2=self.table.page_ranges[pr2_id]
                    cur=pr2.tail_page_sets[pg2].pages[ic].read(idx2)
                i=len(tail_chain)-1
                while i>=0:
                    pr2_id,pg2,idx2=tail_chain[i]
                    pr2=self.table.page_ranges[pr2_id]
                    schema=pr2.tail_page_sets[pg2].pages[sc].read(idx2)
                    col_i=0
                    while col_i<self.table.num_columns:
                        if((schema>>col_i)&1)==1:
                            base_vals[col_i]=pr2.tail_page_sets[pg2].pages[off+col_i].read(idx2)
                        col_i+=1
                    i-=1
                total=total+base_vals[aggregate_column_index]
                count=count+1
            if count==0:return False
            return total
        except:
            return False




    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False
            tot = 0
            seen = 0
            # ****okay so versioned sum starts here
            for rid in rids:
                pd = self.table.page_directory.get(rid)
                if not pd:
                    continue
                pr_id, is_tail, pg, idx = pd
                if is_tail != 0:
                    continue
                pr = self.table.page_ranges[pr_id]
                off = NUM_META_COLUMNS
                vals = [0] * self.table.num_columns
                for c in range(self.table.num_columns):
                    vals[c] = pr.base_page_sets[pg].pages[off + c].read(idx)
                ic = INDIRECTION_COLUMN
                sc = SCHEMA_COLUMN
                trs = []
                cur = pr.base_page_sets[pg].pages[ic].read(idx)
                while True:
                    if cur == -1:
                        break
                    pd2 = self.table.page_directory.get(cur)
                    if not pd2:
                        break
                    pr2_id, is_tail2, pg2, idx2 = pd2
                    if is_tail2 == 0:
                        break
                    trs.append((pr2_id, pg2, idx2))
                    pr2 = self.table.page_ranges[pr2_id]
                    cur = pr2.tail_page_sets[pg2].pages[ic].read(idx2)
                n = len(trs)
                # ****alright so version math happens here slow brain time
                if relative_version <= 0:
                    use = n + relative_version
                else:
                    use = n - relative_version
                if use < 0:
                    use = 0
                if use > n:
                    use = n
                trs_rev = trs[::-1]
                for j in range(use):
                    pr2_id, pg2, idx2 = trs_rev[j]
                    pr2 = self.table.page_ranges[pr2_id]
                    s = pr2.tail_page_sets[pg2].pages[sc].read(idx2)
                    for c in range(self.table.num_columns):
                        if (s >> c) & 1:
                            vals[c] = pr2.tail_page_sets[pg2].pages[off + c].read(idx2)
                tot += vals[aggregate_column_index]
                seen += 1
            if seen == 0:
                return False
            return tot
        except:
            return False


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            cols = [self.NULL_VALUE] * self.table.num_columns
            cols[column] = r[column] + 1
            return self.update(key, *cols)
        return False
