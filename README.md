# Bibycle-availability-real-time

TODO DOCUEMENTATION


Démarrer docker kafka :

sudo dockerd
cd kafka-docker
sudo docker-compose up -d
sudo docker-compose scale kafka=3
sudo docker-compose ps

Arrêter docker kafka

sudo docker-compose stop

 172.17.0.1

Acceder au bash du docker

docker exec -i -t -u root $(docker ps | grep docker_kafka | cut -d' ' -f1) /bin/bash

Créer topic kafka sur kafka

$KAFKA_HOME/bin/kafka-topics.sh --create --partitions 4 --bootstrap-server kafka:9092 --topic nom_topic

en sur la machine hôte

$KAFKA_HOME/bin/kafka-topics.sh --create --partitions 4 --localhost:9092 kafka:9092 --topic nom_topic

Créer consumer

$KAFKA_HOME/bin/kafka-console-consumer.sh --from-beginning --bootstrap-server kafka:9092 --topic=test 




docker run -it --rm --network kafka_docker_net confluentinc/cp-kafka /bin/kafka-console-producer --bootstrap-server kafka:9092 --topic test_topic

tout supprimer 

docker system prune -a

sudo chmod 777 jars_dir && \
docker exec -it spark-shell \
spark-submit \
--packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0" \
--master "spark://172.18.0.10:7077" \
--class Streaming \
--conf spark.jars.ivy=/opt/bitnami/spark/ivy \
ivy/spark-streaming-with-kafka_2.12-1.0.jar
