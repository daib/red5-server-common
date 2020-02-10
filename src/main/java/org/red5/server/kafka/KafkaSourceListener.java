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

    private KafkaProducerWrapper kafkaProducer = null;

    private MessageByteSerializer messageByteSerializer;

    @Override
    public void packetReceived(IBroadcastStream stream, IStreamPacket packet) {
        log.debug("KafkaSourceListener receives packet");
        kafkaProducer.send(stream.getPublishedName(), messageByteSerializer.encode(packet));
    }

    public void init(String bootstrapServer) {
        log.info("Bootstrap server: {}", bootstrapServer);
        //init producer
        kafkaProducer = new KafkaProducerWrapper();
        kafkaProducer.init(bootstrapServer);
        messageByteSerializer = new MessageByteSerializer();

    }

    public void close() {
        kafkaProducer.close();
        kafkaProducer = null;
    }

    public void createTopic(String topic) {
        if (kafkaProducer != null)
            kafkaProducer.creatTopic(topic);
    }
}