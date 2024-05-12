from raft.traffic.controller import TrafficControl, TrafficLight
from raft.actors.network_command import CommandConnectionListener

from threading import Thread
import time

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(threadName)s %(levelname)s - %(message)s',
)

controller_actor = TrafficControl.start(12346, 12347, 0)
controller_actor_proxy = controller_actor.proxy()

ns_light_actor = TrafficLight.start()
ew_light_actor = TrafficLight.start()

def generate_tick(sleep_time):
    tick_time = 1
    while True:
        time.sleep(sleep_time)
        controller_actor_proxy.tick(tick_time)
        tick_time += 1


def run():
    Thread(target=generate_tick, args=[1]).start()

    CommandConnectionListener.start(('localhost', 12345), controller_actor)
    CommandConnectionListener.start(('localhost', 12346), ns_light_actor)
    CommandConnectionListener.start(('localhost', 12347), ew_light_actor)