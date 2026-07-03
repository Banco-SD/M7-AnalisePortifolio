import os
import redis
import json
import pandas as pd
import yfinance as yf
from fastapi import FastAPI
import threading
from worker import iniciar_worker
from cron_snapshot import iniciar_agendador
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Módulo de Análise e Portfólio")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

cache = redis.from_url(REDIS_URL, decode_responses=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread_worker = threading.Thread(target=iniciar_worker, daemon=True)
    thread_worker.start()
    
    thread_cron = threading.Thread(target=iniciar_agendador, daemon=True)
    thread_cron.start()
    
    print("[*] Serviços em background (Worker AMQP e Cron) iniciados com sucesso.")
    
    yield 

    print("[*] Encerrando a API e os serviços em background...")

app = FastAPI(title="Módulo de Análise e Portfólio", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://m8-frontend.vercel.app",
        "https://gateway-xvfk.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"],
)

@app.get("/api/portfolio/{usuario_id}")
def analisar_portfolio(usuario_id: str):
    
    # Tenta servir os dados pré-calculados pelo cron_snapshot
    snapshot_diario = cache.get(f"snapshot:{usuario_id}")
    if snapshot_diario:
        return json.loads(snapshot_diario)
    
    # Busca o histórico completo de transações do usuário no Redis
    chave_transacoes = f"extrato:{usuario_id}"
    transacoes_str = cache.lrange(chave_transacoes, 0, -1)
    
    if not transacoes_str:
        return {"usuario_id": usuario_id, "mensagem": "Nenhuma transação encontrada."}

    # Carrega as transações em um DataFrame Pandas para processamento matricial eficiente
    transacoes = [json.loads(t) for t in transacoes_str]
    df = pd.DataFrame(transacoes)
    
    # Ajusta o sinal matemático
    df['quantidade_ajustada'] = df.apply(
        lambda linha: linha['quantidade'] if linha['tipo'] == 'COMPRA' else -linha['quantidade'], 
        axis=1
    )
    
    # Calcula o volume financeiro exato gasto no momento da operação
    df['valor_operacao_historico'] = df['quantidade_ajustada'] * df['preco_unitario']
    
    # Agrupa as operações por dia e faz a soma cumulativa para desenhar 
    # a curva de crescimento do patrimônio do usuário ao longo do tempo.
    df['data'] = pd.to_datetime(df['data'])
    evolucao_diaria = df.groupby(df['data'].dt.strftime('%Y-%m-%d'))['valor_operacao_historico'].sum().reset_index()
    evolucao_diaria = evolucao_diaria.sort_values('data')
    evolucao_diaria['patrimonio_acumulado'] = evolucao_diaria['valor_operacao_historico'].cumsum()
    historico_json = evolucao_diaria[['data', 'patrimonio_acumulado']].rename(columns={'patrimonio_acumulado': 'valor'}).to_dict(orient='records')

    # Utiliza o desvio padrão da variação percentual diária para 
    # classificar a agressividade da carteira do investidor.
    volatilidade = evolucao_diaria['patrimonio_acumulado'].pct_change().std()
    
    if pd.isna(volatilidade):
        score_risco = "Dados Insuficientes"
    elif volatilidade < 0.015:  # Menos de 1.5% de oscilação diária
        score_risco = "Baixo (Conservador)"
    elif volatilidade < 0.03:   # Entre 1.5% e 3%
        score_risco = "Médio (Moderado)"
    else:                       # Acima de 3%
        score_risco = "Alto (Agressivo)"

    # Consolida a quantidade total atual de cada ativo na carteira
    resumo = df.groupby('ativo').agg(
        quantidade_total=('quantidade_ajustada', 'sum'),
        custo_historico=('valor_operacao_historico', 'sum')
    ).reset_index()
    # Remove ativos que foram totalmente vendidos (posição zerada)
    resumo = resumo[resumo['quantidade_total'] > 0]
    
    precos_atuais = []
    for ativo in resumo['ativo']:
        chave_ultimo_preco = f"preco_mercado:{ativo}"
        try:
            #Busca a cotação real na B3 usando o Yahoo Finance
            ticker = yf.Ticker(f"{ativo}.SA")
            preco_real = float(ticker.history(period="1d")['Close'].iloc[-1])
            
            #P ersiste o último preço válido no Redis como segurança
            cache.set(chave_ultimo_preco, preco_real)
            precos_atuais.append(preco_real)
            
        except Exception:
            # Como fallback se a API cair, lê o último preço salvo no cache.
            ultimo_preco_salvo = cache.get(chave_ultimo_preco)
            
            if ultimo_preco_salvo:
                print(f"[!] Falha no Yahoo. Usando último preço de mercado salvo para {ativo}: R$ {ultimo_preco_salvo}")
                precos_atuais.append(float(ultimo_preco_salvo))
            else:
                # Em último caso, usa o preço médio histórico pago pelo investidor.
                preco_medio_historico = resumo.loc[resumo['ativo'] == ativo, 'custo_historico'].values[0] / resumo.loc[resumo['ativo'] == ativo, 'quantidade_total'].values[0]
                print(f"[!] Sem histórico no Redis. Usando preço médio de compra para {ativo}: R$ {preco_medio_historico}")
                precos_atuais.append(preco_medio_historico)
                
    # Atualiza o patrimônio baseando-se nos preços de mercado atuais
    resumo['preco_mercado'] = precos_atuais
    resumo['valor_atualizado'] = resumo['quantidade_total'] * resumo['preco_mercado']
    
    patrimonio_total = resumo['valor_atualizado'].sum()
    resumo['percentual_carteira'] = (resumo['valor_atualizado'] / patrimonio_total) * 100

    # Busca os alvos definidos pelo usuário no Módulo de Perfil e compara com a realidade
    alvos_str = cache.get(f"alvos:{usuario_id}")
    alvos_usuario = json.loads(alvos_str) if alvos_str else {}
    
    recomendacoes = []
    for indice, linha in resumo.iterrows():
        ativo = linha['ativo']
        perc_atual = linha['percentual_carteira']
        perc_alvo = alvos_usuario.get(ativo, 0.0)
        
        # Calcula a distância entre o que o usuário tem e o que ele deseja ter
        diferenca = perc_alvo - perc_atual
        
        # Aplica uma margem de tolerância de 2% 
        if diferenca > 2.0:
            recomendacoes.append({"ativo": ativo, "acao": "COMPRAR", "desvio": diferenca, "motivo": f"Abaixo da meta. Atual: {perc_atual:.1f}% | Alvo: {perc_alvo}%"})
        elif diferenca < -2.0:
            recomendacoes.append({"ativo": ativo, "acao": "VENDER", "desvio": diferenca, "motivo": f"Acima da meta. Atual: {perc_atual:.1f}% | Alvo: {perc_alvo}%"})
        else:
            recomendacoes.append({"ativo": ativo, "acao": "MANTER", "desvio": diferenca, "motivo": "Alocação ideal."})
    
    ativos_dit = resumo.set_index('ativo').to_dict(orient='index')
    
    # Retorna o payload estruturado para consumo do Frontend e do Cron Job
    return {
        "usuario_id": usuario_id,
        "patrimonio_total_atualizado": round(patrimonio_total, 2),
        "perfil_de_risco": score_risco,
        "evolucao_patrimonial": historico_json, 
        "recomendacoes": recomendacoes,
        "ativos": ativos_dit
    }