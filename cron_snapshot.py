import os
import redis
import json
import requests
import pika
from apscheduler.schedulers.blocking import BlockingScheduler

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

API_URL = os.environ.get("API_URL", "http://localhost:8000")

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")

cache = redis.from_url(REDIS_URL, decode_responses=True)


def gerar_snapshots_da_bolsa():
    """
    Função principal executada pelo relógio cron. 
    1. Localiza quem possui investimentos ativos.
    2. Aciona o endpoint de análise de cada usuário.
    3. Avalia o risco da carteira calculada.
    4. Dispara alertas para os usuários fora do limite aceitável de meta.
    5. Grava a nova foto no Redis para leituras de baixa latência (Snapshot).
    """
    print("[*] A iniciar a rotina de consolidação periódica do mercado...")
    
    # Busca dinamicamente todos os usuários através das chaves de extrato presentes no banco
    chaves_de_extrato = cache.keys("extrato:*")
    
    if not chaves_de_extrato:
        print("[-] Nenhum utilizador com transações ativo no sistema.")
        return

    # Tenta conectar ao RabbitMQ para orquestrar as notificações. 
    try:
        parametros = pika.URLParameters(RABBITMQ_URL)
        conexao = pika.BlockingConnection(parametros)
        canal = conexao.channel()
        canal.queue_declare(queue='fila.alerta.disparar')
    except Exception as e:
        print(f"[x] Falha na ligação ao RabbitMQ: {e}")
        return

    # Isola o ID extraindo-o do formato da chave
    usuarios_ativos = [chave.split(":")[1] for chave in chaves_de_extrato]
    
    for usuario_id in usuarios_ativos:
        print(f"A processar análise de mercado para o utilizador: {usuario_id}")
        
        try:
            # Chama a própria API para acionar o motor analítico e o Yahoo Finance
            resposta = requests.get(f"{API_URL}/api/portfolio/{usuario_id}")
            
            if resposta.status_code == 200:
                dados_consolidados = resposta.json()
                precisa_alerta = False
                ativos_desbalanceados = []
                
                # Itera sobre todas as sugestões que a API acabou de gerar
                for rec in dados_consolidados.get('recomendacoes', []):
                    
                    # Usa o valor absoluto para capturar o excesso ou carência em relação à meta 
                    if abs(rec.get('desvio', 0)) > 5.0:
                        precisa_alerta = True
                        ativos_desbalanceados.append(rec['ativo'])
                        
                # Se algum ativo estiver muito desequilibrado, se comunica com o módulo de notificações
                if precisa_alerta:
                    payload_alerta = {
                        "user_id": usuario_id,
                        "tipo_alerta": "REBALANCEAMENTO_URGENTE",
                        "mensagem": f"Atenção! A sua carteira desbalanceou mais de 5% nos ativos: {', '.join(ativos_desbalanceados)}. Aceda à aplicação para rebalancear."
                    }
                    canal.basic_publish(
                        exchange='',
                        routing_key='fila.alerta.disparar',
                        body=json.dumps(payload_alerta)
                    )
                    print(f"[!] Alerta de desvio crítico enviado para a fila do utilizador {usuario_id}.")
                
                for rec in dados_consolidados.get('recomendacoes', []):
                    rec.pop('desvio', None)

                cache.setex(f"snapshot:{usuario_id}", 7200, json.dumps(dados_consolidados))
                print(f"Snapshot dinâmico de {usuario_id} guardado no Redis.")
                
        except Exception as e:
            print(f"[x] Erro ao calcular dados para {usuario_id}: {e}")
            
    conexao.close()

if __name__ == '__main__':
    print("[*] Servidor de Agendamento Ativo")
    
    # Inicia um relógio bloqueante 
    agendador = BlockingScheduler()
    
    # Dispara no minuto zero, das 10h às 18h, de Seg a Sex
    # que é o horário de funcionamento da bolsa
    agendador.add_job(
        gerar_snapshots_da_bolsa, 
        'cron', 
        day_of_week='mon-fri', 
        hour='10-18', 
        minute='0'
    )
    
    print("[*] Relógio sincronizado com o horário de funcionamento da B3. A aguardar o pregão...")
    agendador.start()