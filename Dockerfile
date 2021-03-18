FROM godatadriven/pyspark

RUN apt-get install -y wget
ENV POST_URL https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN wget ${POST_URL}
RUN mkdir -p $SPARK_HOME/jars/
RUN mv postgresql-42.2.5.jar $SPARK_HOME/jars/postgresql-42.2.5.jar

WORKDIR /job
COPY . .
ENTRYPOINT python3 /job/main.py
