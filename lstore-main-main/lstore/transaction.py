from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self._log = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        self._log = []
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            if getattr(query, "__name__", "") == "insert":
                qobj = getattr(query, "__self__", None)
                if qobj is not None:
                    table = qobj.table if hasattr(qobj, "table") else qobj
                    if hasattr(table, "key") and table.key < len(args):
                        self._log.append((table, args[table.key]))
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for table, pk in reversed(self._log):
            try:
                from lstore.query import Query
                Query(table).delete(pk)
            except:
                pass
        return False

    
    def commit(self):
        # TODO: commit to database
        return True
