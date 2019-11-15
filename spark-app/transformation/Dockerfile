FROM apache-spark:2.4.3
COPY . /app
CMD /app/entrypoint.sh $KAFKA_HOST $KAFKA_TOPICS $BATCH_SIZE