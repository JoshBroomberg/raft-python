import sqlite3

class PersistedState():
    """
    Stored current_term and voted_for in a sqlite db
    Has methods set_current_term, get_current_term, set_voted_for, get_voted_for
    """
    
    def __init__(self, db_path, current_term=None, voted_for=None):
        self.db_path = db_path
        self._create_table()

        if current_term is not None:
            self.set_current_term(current_term)
        
        if voted_for is not None:
            # TODO: turn this into a map
            self.set_voted_for(voted_for)
    
    def _create_table(self):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()

            c.execute('''CREATE TABLE IF NOT EXISTS state
                         (key TEXT PRIMARY KEY, value INTEGER)''')
    
    def set_current_term(self, term):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute('INSERT OR REPLACE INTO state VALUES (?, ?)', ('current_term', term))
        
    def get_current_term(self):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute('SELECT value FROM state WHERE key = ?', ('current_term',))
            row = c.fetchone()
            if row:
                return int(row[0]) if row[0] is not None else 0
            return 0 # TODO: validate this is sane
    
    def set_voted_for(self, node_id):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute('INSERT OR REPLACE INTO state VALUES (?, ?)', ('voted_for', node_id))
    
    def get_voted_for(self):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute('SELECT value FROM state WHERE key = ?', ('voted_for',))
            row = c.fetchone()
            if row:
                return int(row[0]) if row[0] is not None else None
            return None