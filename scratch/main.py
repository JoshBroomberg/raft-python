from raft.container import Container
from raft.actors.keyval import KeyValue

def run():
    container = Container()
    container.init_resources()
    keyvalue_ref = container.keyvalue_actor().proxy()
    
    keyvalue_ref.set('key', 'val')
    print(keyvalue_ref)

    # keyvalue_actor_ref.set('key', 'val')
    # keyvalue_actor_ref.get('key')
    # snapshot_name = keyvalue_actor_ref.create_snapshot()
    # print(snapshot_name.get())

    # keyvalue_actor_ref.stop()