KAFKA_HOST=${1:-"localhost:9092"}
KAFKA_TOPICS=${2:-tweets}
BATCH_SIZE=${3:-10}

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 \
    /app/app.py $KAFKA_HOST $KAFKA_TOPICS $BATCH_SIZE