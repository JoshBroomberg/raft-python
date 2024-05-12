import pykka
from dependency_injector.wiring import inject

# Define an actor that uses the ApiService
class NodeActor(pykka.ThreadingActor):
    @inject
    def __init__(self, service):
        super().__init__()
        self.service = service

    def on_receive(self, message):
        print("service result", self.service.test())
        print("received message", message)

