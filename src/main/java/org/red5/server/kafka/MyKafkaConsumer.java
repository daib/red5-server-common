package org.red5.server.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.red5.server.kafka.KafkaProto.KafkaRTMPMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class MyKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(MyKafkaConsumer.class);

    private final String bootstrapServer = "localhost:9092";

    private Properties properties;

    private final static String TOPIC = "test";

    private KafkaConsumer<String, byte[]> kafkaConsumer;

    public void init() {
        log.info("init mykafkaconsumer");
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "MyKafkaConsumer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaConsumer = new KafkaConsumer<String, byte[]>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC));
        log.info("end init kafkaconsumer");
    }

    public ConsumerRecords<String, byte[]> receive() {
        //log.info("receive   MyKafkaConsumer");
        final int giveUp = 20;
        int noRecordsCount = 0;

        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(1);

        kafkaConsumer.commitAsync();
        return records;

    }

    public void close() {
        kafkaConsumer.commitSync();
        kafkaConsumer.close();
    }

}
