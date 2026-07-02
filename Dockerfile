# Usa uma imagem oficial do Python, bem leve
FROM python:3.10-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de dependências e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o resto do seu código para dentro do contêiner
COPY . .

# Expõe a porta que o FastAPI vai rodar
EXPOSE 8000

# Comando para rodar a sua API quando o contêiner iniciar
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]