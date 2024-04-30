#!/usr/bin/env python3

# Algoritmos Distribuidos
# Componente Básico
# (baseado em consumidor.py)
# Prof. Luiz Lima Jr.

from pika import BlockingConnection
import sys

# entrega a mensagem
def recebendo(msg, canal):
    print(f'mensagem recebida: "{msg}"')

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

conexao = BlockingConnection()
canal = conexao.channel()

canal.queue_declare(queue=idx, auto_delete=True)

def callback(canal, metodo, props, corpo):
    recebendo(corpo.decode(), canal)

canal.basic_consume(queue=idx,
                    on_message_callback=callback,
                    auto_ack=True)
try:
    print(f"{idx} aguardando mensagens")
    canal.start_consuming()
except KeyboardInterrupt:
    canal.stop_consuming()

conexao.close()

