import zmq
import json

class NameServiceClient:
    def __init__(self, endpoint="tcp://192.168.40.203:5555"):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(endpoint)
        
    def _send(self, payload):
        self.socket.send_string(json.dumps(payload))
        response = self.socket.recv_string()
        return json.loads(response)

    def bind(self, nome, endereco):
        return self._send({"op": "bind", "nome": nome, "endereco": endereco})

    def lookup(self, nome):
        return self._send({"op": "lookup", "nome": nome})

    def unbind(self, nome):
        return self._send({"op": "unbind", "nome": nome})

    def register(self, nome, tipo):
        return self._send({"op": "register", "nome": nome, "tipo": tipo})

    def discover(self, tipo):
        return self._send({"op": "discover", "tipo": tipo})

# --- Fluxo de Teste ---
if __name__ == "__main__":
    client = NameServiceClient()

    print("1. Testando BIND:")
    print("Bind Peer1:", client.bind("peer1", "tcp://192.168.1.10:6001"))
    print("Bind Peer2:", client.bind("peer2", "tcp://192.168.1.11:6002"))
    print("Bind WebServer:", client.bind("web_server", "tcp://192.168.1.50:80"))
    print("-" * 40)

    print("2. Testando LOOKUP:")
    print("Lookup Peer1:", client.lookup("peer1"))
    print("Lookup Inexistente:", client.lookup("peer3"))
    print("-" * 40)

    print("3. Testando REGISTER (Atributos/Tipos):")
    print("Register Peer1 como 'peer':", client.register("peer1", "peer"))
    print("Register Peer2 como 'peer':", client.register("peer2", "peer"))
    print("Register WebServer como 'service':", client.register("web_server", "service"))
    print("Register Erro (Inexistente):", client.register("peer3", "peer"))
    print("-" * 40)

    print("4. Testando DISCOVER:")
    print("Discover 'peer':", client.discover("peer"))
    print("-" * 40)

    print("5. Testando UNBIND:")
    print("Unbind Peer1:", client.unbind("peer1"))
    print("Lookup Peer1 pós-unbind:", client.lookup("peer1"))
    print("Discover 'peer' pós-unbind (deve listar apenas peer2):", client.discover("peer"))
