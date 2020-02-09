package org.red5.server.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerWrapper {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerWrapper.class);

    private KafkaProducer<String, byte[]> producer = null;

    public void init(String kafkaBootstrapServers) {
        log.info("Initializing KafkaProducerWrapper at bootstrap server: {}", kafkaBootstrapServers);
        /*
         * Defining producer properties.
         */
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "100000000");

        /*
         * Creating a Kafka Producer object with the configuration above.
         */
        producer = new KafkaProducer<>(producerProperties);
    }

    public void send(String topic, byte[] payload) {
        log.debug("Sending Kafka message: {} bytes to topic {}", payload.length, topic);
        producer.send(new ProducerRecord<>(topic, payload));
    }

    public void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }

    public void creatTopic(String topic) {
        if (producer != null) {
            log.info("Creationg topic {}", topic);
            producer.partitionsFor(topic);
        }
    }
}
