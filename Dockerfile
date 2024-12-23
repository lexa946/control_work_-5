FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y default-jdk

COPY analyzer.py analyzer.py
COPY start.sh start.sh
COPY log_generator.py log_generator.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

CMD ["./start.sh"]