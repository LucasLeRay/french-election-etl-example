FROM godatadriven/pyspark

RUN pip install psycopg2-binary

WORKDIR /job
COPY . .
ENTRYPOINT python3 /job/main.py
