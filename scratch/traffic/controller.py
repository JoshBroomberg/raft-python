import time

import pykka
from dependency_injector.wiring import inject

NS = 'ns'
EW = 'ew'

R = 'red'
Y = 'yellow'
G = 'green'

class TrafficLight(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.state = None
    
    def change_state(self, state: str) -> None:
        self.state = state
    

class TrafficControl(pykka.ThreadingActor):
    def __init__(self, ns_light_port, ew_light_port, start_time):
        super().__init__()

        self.light_ports = {
            NS: ns_light_port,
            EW: ew_light_port
        }

        self.light_state_time = {
            NS: 0,
            EW: 0
        }

        self.light_state = {
            NS: G,
            EW: R
        }

        self.pending_button_push = {
            NS: False,
            EW: False
        }

        self.last_tick_time = start_time

    def tick(self, tick_time) -> None:
        self.light_state_time[NS] += tick_time - self.last_tick_time
        self.light_state_time[EW] += tick_time - self.last_tick_time
        self.last_tick_time = tick_time

        NS_min_green_time = 15 if self.pending_button_push[NS] else 60
        if self.light_state[NS] == G and self.light_state_time[NS] >= NS_min_green_time:
            self.change_state(NS, Y)
        elif self.light_state[NS] == Y and self.light_state_time[NS] >= 5:
            self.change_state(NS, R)
            self.change_state(EW, G)
        
        EW_min_green_time = 15 if self.pending_button_push[EW] else 30
        if self.light_state[EW] == G and self.light_state_time[EW] >= EW_min_green_time:
            self.change_state(EW, Y)
        elif self.light_state[EW] == Y and self.light_state_time[EW] >= 5:
            self.change_state(EW, R)
            self.change_state(NS, G)

    def button_push(self, light: str) -> None:
        print(f"Button pressed on {light} at {self.last_tick_time}")
        self.pending_button_push[light] = True

    def change_state(self, light: str, state: str) -> None:
        print(f"Changing {light} to {state} {self.last_tick_time}")
        self.light_state[light] = state
        self.light_state_time[light] = 0

        if state == R:
            self.pending_button_push[light] = False