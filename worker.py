import os
import pika
import redis
import json

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

# Inicializa o cliente de conexão com o banco de dados Redis.
cache = redis.from_url(REDIS_URL, decode_responses=True)

def processar_ordem(ch, method, properties, body):
    """
    Callback acionado sempre que uma nova ordem de compra ou venda é executada.
    Consome mensagens vindas do Módulo de Investimentos via RabbitMQ.
    """
    # Converte o corpo da mensagem em um dicionário Python
    nova_ordem = json.loads(body)
    usuario_id = nova_ordem.get('user_id')
    
    # Define a chave que armazena a lista encadeada deste usuário no Redis
    chave_transacoes = f"extrato:{usuario_id}"
    
    # Adiciona a nova operação de forma persistente e ordenada no fim da lista 
    cache.rpush(chave_transacoes, json.dumps(nova_ordem))
    
    # Apaga o registro atual e recalcula os dados
    cache.delete(f"snapshot:{usuario_id}")
    
    print(f"[x] Transação registrada e snapshot invalidado para {usuario_id}")

def processar_metas(ch, method, properties, body):
    """
    Callback acionado sempre que o usuário atualiza seus alvos de alocação da carteira.
    Consome mensagens vindas do Módulo de Perfil via RabbitMQ.
    """
    # Converte a mensagem em dicionário
    novas_metas = json.loads(body)
    usuario_id = novas_metas.get('user_id')
    
    # Extrai o dicionário de ativos e alvos
    alvos = novas_metas.get('alvos', {})
    
    # Sobrescreve as metas anteriores do usuário no Redis
    cache.set(f"alvos:{usuario_id}", json.dumps(alvos))
    
    # Se as metas do usuário mudaram, o motor de recomendação precisa gerar novas instruções.
    # Apagamos o snapshot diário para forçar um novo processamento analítico na API.
    cache.delete(f"snapshot:{usuario_id}")
    
    print(f"[x] Metas atualizadas para {usuario_id}: {alvos}")


# Tenta ler a URL de conexão do broker. Se não houver, usa o local
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")

def iniciar_worker():
    """Configura o canal de comunicação assíncrona e inicia o loop consumidor."""
    # Configura os parâmetros de rede através da URL do broker
    parametros = pika.URLParameters(RABBITMQ_URL)
    conexao = pika.BlockingConnection(parametros)
    canal = conexao.channel()
    
    # Assegura a existência das filas no RabbitMQ. Se não existirem, o broker as cria.
    canal.queue_declare(queue='fila.ordem.executada')
    canal.queue_declare(queue='fila.metas.atualizadas')
    
    # Associa formalmente cada fila à sua respectiva função de processamento 
    canal.basic_consume(queue='fila.ordem.executada', on_message_callback=processar_ordem, auto_ack=True)
    canal.basic_consume(queue='fila.metas.atualizadas', on_message_callback=processar_metas, auto_ack=True)

    print(' [*] Trabalhador rodando e escutando múltiplas filas no RabbitMQ...')
    
    # Trava o terminal e entra em loop infinito escutando e aguardando a chegada de eventos
    canal.start_consuming()

if __name__ == '__main__':
    iniciar_worker()