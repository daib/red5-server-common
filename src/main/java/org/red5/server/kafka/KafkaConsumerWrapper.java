package org.red5.server.kafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerWrapper {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerWrapper.class);

    private KafkaConsumer<String, byte[]> kafkaConsumer = null;

    public void init(String kafkaBootstrapServers, String topics, String zookeeperGroupId) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, zookeeperGroupId);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "100000000");
        kafkaConsumer = new KafkaConsumer<String, byte[]>(consumerProperties);
        kafkaConsumer.assign(Arrays.asList(new TopicPartition(topics, 0)));

    }

    public ConsumerRecords<String, byte[]> receive() {
        return kafkaConsumer.poll(100);
    }

    public void seekToEnd(TopicPartition topicPartition) {
        log.debug("Seeking to the end of topic partition {}.", topicPartition);
        kafkaConsumer.seekToEnd(Collections.singleton(topicPartition));
    }

    public void seekToEnd(String topic, int partition) {
        log.debug("Seeking to the end of topic {} partition {}.", topic, partition);
        seekToEnd(new TopicPartition(topic, partition));
    }

    public void seek(TopicPartition topicPartition, long offset) {
        log.debug("Seeking topic partition {} to offset {}", topicPartition, offset);
        kafkaConsumer.seek(topicPartition, offset);
    }

    public void seek(String topic, int partition, long offset) {
        log.debug("Seeking topic {} partition {} to offset " + offset, topic, partition);
        seek(new TopicPartition(topic, partition), offset);
    }

    public void commitOffset(String topic, int partition, long offset) {
        Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

        commitMessage.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));

        kafkaConsumer.commitSync(commitMessage);

        log.debug("Offset committed to Kafka.");
    }

    public void close() {
        kafkaConsumer.commitSync();
        kafkaConsumer.close();
    }
}
