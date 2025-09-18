# =============================
# Procfile avançado - multi-processos sincronizados
# =============================

# 🔄 Etapa de migração automática (executa antes de iniciar serviços)
release: alembic upgrade head

# 🤖 Bot principal: captura leads e envia eventos para FB/GA + Typebot
bot: python bot.py

# ⚙️ Worker: processa filas de eventos, retro-feed e retries
worker: python worker.py

# 📊 Admin: painel HTTP/Prometheus (monitoramento e métricas)
admin: uvicorn admin_service:app --host 0.0.0.0 --port 8000 --log-level info

# 🔁 Retro-feed: reenvia leads antigos para novos pixels
retrofeed: python tools/retrofeed.py

# 🔥 Warmup: reprocessa leads históricos para enriquecer score e priorização
warmup: python tools/warmup.py

# 📦 DLQ: processa eventos que falharam (dead-letter queue), com retry e logging detalhado
dlq: python tools/dlq_processor.py

# ⏰ Scheduler: executa tarefas periódicas (limpeza de filas, métricas e atualizações automáticas)
scheduler: python tools/scheduler.py

# 🛠️ Migração manual opcional (caso precise rodar forçado)
migrate: alembic upgrade head

# =============================
# Observações:
# - release garante que migrations rodem ANTES do app subir
# - cada processo pode ser escalado individualmente pelo Railway
#   (ex.: 2 instâncias de worker e 1 de bot para alta taxa de eventos)
# - admin roda em 0.0.0.0:8000 (defina como Healthcheck no Railway)
# =============================