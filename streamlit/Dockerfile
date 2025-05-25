FROM python:3.9-slim

WORKDIR /app

# Installation des dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie des fichiers d'application
COPY ./app /app

# Exposition du port Streamlit
EXPOSE 8501

# Commande pour lancer l'application
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]