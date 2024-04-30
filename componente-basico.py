#!/usr/bin/env python3

from pika import BlockingConnection
import sys

firstTime = True

# entrega a mensagem
def recebendo(msg, origem, canal):
    print(f'mensagem recebida: "{msg}"')
    global firstTime
    if firstTime:
        destinos = Nx[:]
        destinos.remove(origem)
        for vizinho in Nx:
            envia(idx+":"+msg, vizinho, canal)
        firstTime = False


# envia msg a dest
def envia(msg, dest, canal):
    canal.basic_publish(exchange='',
                        routing_key=dest,
                        body=msg)

if len(sys.argv) < 2:
    print(f'USO: {sys.argv[0]} <id>')
    exit(1)

idx = sys.argv[1]  # identificador do componente
Nx = sys.argv[2:] # vizinhos
Nx.append("STARTER")

print("idx = ", idx)
print("Nx = ", Nx)

conexao = BlockingConnection()
canal = conexao.channel()

canal.queue_declare(queue=idx, auto_delete=True)
for vizinho in Nx:
    canal.queue_declare(queue=vizinho, auto_delete=True)

def callback(canal, metodo, props, corpo):
    msg = corpo.decode().split(":")
    recebendo(msg[1], msg[0], canal)

canal.basic_consume(queue=idx,
                    on_message_callback=callback,
                    auto_ack=True)
try:
    print(f"{idx} aguardando mensagens")
    canal.start_consuming()
except KeyboardInterrupt:
    canal.stop_consuming()

conexao.close()

