package org.red5.server.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.api.stream.IStreamListener;
import org.red5.server.api.stream.IStreamPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class KafkaSourceListener implements IStreamListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceListener.class);

    private Properties propertiesProducer;

    private KafkaProducer<String, byte[]> kafkaProducer;

    private MessageByteSerializer messageByteSerializer;

    @Override
    public void packetReceived(IBroadcastStream stream, IStreamPacket packet) {
        log.info("This is Kafka Source Listener");
        byte[] data = messageByteSerializer.encode(packet);

        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(stream.getPublishedName(), data);
        kafkaProducer.send(record);
        kafkaProducer.flush();
    }

    public void init(String bootstrapServer) {
        log.info("init kafkasourcelistener");
        //init producer
        propertiesProducer = new Properties();
        propertiesProducer.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        propertiesProducer.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesProducer.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducer = new KafkaProducer<String, byte[]>(propertiesProducer);

        messageByteSerializer = new MessageByteSerializer();
        log.info("end init kafkasourcelistener");

    }

    public void close() {
        kafkaProducer.close();
    }

    void createTopic(String topic) {
        kafkaProducer.partitionsFor(topic);
    }
}