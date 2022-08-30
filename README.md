# Bibycle-availability-real-time

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