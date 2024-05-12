import logging
import pickle
import time

import pykka
from dependency_injector.wiring import inject


class KeyValue(pykka.ThreadingActor):
    @inject
    def __init__(self, snapshot_name = None):
        super().__init__()
        self.logger = logging.getLogger(__class__.__name__)
        self.snapshot_name = snapshot_name

    def on_start(self) -> None:
        if self.snapshot_name:
            self.logger.info(f'Loading snapshot {self.snapshot_name}')
            self._load_snapshot(self.snapshot_name)
        else:
            self.logger.info('Creating new data store')
            self._data = {}

    def get(self, key):
        self.logger.info(f"Getting key '{key}'")
        return self._data.get(key)
    
    def set(self, key, value):
        self.logger.info(f"Setting '{key}' to '{value}'")
        self._data[key] = value
    
    def delete(self, key):
        self.logger.info(f"Deleting key '{key}'")
        del self._data[key]

    def create_snapshot(self):
        """Snapshot all data to a pickle file in /tmp using current milli timestamp as filename"""
        filename = f'/tmp/{int(time.time() * 1000)}.pickle'
        with open(filename, 'wb') as f:
            pickle.dump(self._data, f)
        return filename

    def _load_snapshot(self, snapshot_name):
        with open(snapshot_name, 'rb') as f:
            self._data = pickle.load(f)

