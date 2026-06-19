import zmq
import json

# Armazenamento em memória
# names_db: { nome: endereco }
names_db = {}
# types_db: { nome: tipo }
types_db = {}

def handle_request(request):
    try:
        data = json.loads(request)
        op = data.get("op")
        
        if op == "bind":
            nome = data.get("nome")
            endereco = data.get("endereco")
            if not nome or not endereco:
                return {"status": "erro", "message": "Nome ou endereço ausentes."}
            names_db[nome] = endereco
            return {"status": "ok"}
            
        elif op == "lookup":
            nome = data.get("nome")
            if nome in names_db:
                return {"status": "ok", "endereco": names_db[nome]}
            return {"status": "erro", "message": f"Nome '{nome}' não encontrado."}
            
        elif op == "unbind":
            nome = data.get("nome")
            if nome in names_db:
                del names_db[nome]
                if nome in types_db:
                    del types_db[nome]
                return {"status": "ok"}
            return {"status": "erro", "message": f"Nome '{nome}' não existe."}
            
        elif op == "register":
            nome = data.get("nome")
            tipo = data.get("tipo")
            if nome not in names_db:
                return {"status": "erro", "message": f"Nome '{nome}' não existe. Faça o bind primeiro."}
            types_db[nome] = tipo
            return {"status": "ok"}
            
        elif op == "discover":
            tipo = data.get("tipo")
            resultados = []
            for nome, t in types_db.items():
                if t == tipo and nome in names_db:
                    resultados.append({"nome": nome, "endereco": names_db[nome]})
            return {"status": "ok", "processos": resultados}
            
        else:
            return {"status": "erro", "message": "Operação inválida."}
            
    except Exception as e:
        return {"status": "erro", "message": str(e)}

def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    # Vincula o servidor na porta 5555
    socket.bind("tcp://*:5555")
    
    print("Serviço de Nomes/Diretório iniciado na porta 5555...")
    
    while True:
        # Recebe a requisição do cliente
        message = socket.recv_string()
        
        # Processa e gera a resposta
        response = handle_request(message)
        
        # Envia a resposta de volta
        socket.send_string(json.dumps(response))

if __name__ == "__main__":
    main()