FROM spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir \
        cassandra-driver \
        neo4j \
        redis

USER spark
