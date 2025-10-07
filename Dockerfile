FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Download spacy model
RUN python -m spacy download en_core_web_sm

EXPOSE 8000

CMD ["python", "src/orchestrator/omni_orchestrator.py"]