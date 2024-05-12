import sqlite3
from typing import Optional

from raft.node.commands import LogEntry

class RaftLog():
    """
    Implements a log data structure backed by sqlite.
    Each entry has a term (int) and command (str)
    Has methods to get entry at index and to insert entries at index
    The insert at index truncates everything above that index
    """

    def __init__(self, db_path='/tmp/log.db', entries: Optional[list[LogEntry]]=None):
        self.db_path = db_path
        self._create_table()

        if entries is not None:
            self.insert_entries_at_index(0, entries)
    
    def _create_table(self):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()

            c.execute('''CREATE TABLE IF NOT EXISTS log
                         (ind INTEGER PRIMARY KEY, term INTEGER, command TEXT)''')
    
    def get_entry_at_index(self, index) -> LogEntry:
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute('SELECT term, command FROM log WHERE ind = ?', (index,))
            row = c.fetchone()
            if row:
                return LogEntry(term=row[0], command=row[1])
            return None
    
    def insert_entries_at_index(self, index, entries: LogEntry):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute('BEGIN TRANSACTION')
            c.execute('DELETE FROM log WHERE ind >= ?', (index,))
            for i, entry in enumerate(entries):
                c.execute('INSERT INTO log VALUES (?, ?, ?)', (index + i, entry.term, entry.command))
            c.execute('COMMIT')
    
    def get_length(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM log')
            return c.fetchone()[0]
    
    def get_entries(self, from_index=0) -> list[LogEntry]:
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute('SELECT term, command FROM log WHERE ind >= ? ORDER BY ind ASC', (from_index,))
            return [LogEntry(term=row[0], command=row[1]) for row in c.fetchall()]
    
    def get_entry_at_index(self, index):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute('SELECT term, command FROM log WHERE ind = ?', (index,))
            row = c.fetchone()
            if row:
                return LogEntry(term=row[0], command=row[1])
            return None