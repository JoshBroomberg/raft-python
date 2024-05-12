import pytest
from raft.node.persisted_state import PersistedState
import os
import sqlite3

@pytest.fixture
def test_db_path():
    db_path = "/tmp/test.db"
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass
    
    yield db_path
    os.remove(db_path)

def test_initialization(test_db_path):
    ps = PersistedState(test_db_path, current_term=1, voted_for=2)
    with sqlite3.connect(test_db_path) as conn:
        c = conn.cursor()
        c.execute('SELECT value FROM state WHERE key = ?', ('current_term',))
        assert c.fetchone()[0] == 1
        c.execute('SELECT value FROM state WHERE key = ?', ('voted_for',))
        assert c.fetchone()[0] == 2
    
    assert ps.get_current_term() == 1
    assert ps.get_voted_for() == 2

def test_set_get_current_term(test_db_path):
    ps = PersistedState(test_db_path)
    ps.set_current_term(5)
    assert ps.get_current_term() == 5

def test_set_get_voted_for(test_db_path):
    ps = PersistedState(test_db_path)
    ps.set_voted_for(3)
    assert ps.get_voted_for() == 3