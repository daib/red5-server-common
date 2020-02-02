package org.red5.server.kafka;

import com.google.protobuf.ByteString;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.mina.core.buffer.IoBuffer;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.api.stream.IStreamListener;
import org.red5.server.api.stream.IStreamPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import org.red5.server.kafka.KafkaProto.KafkaRTMPMessage;

public class KafkaSourceListener implements IStreamListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceListener.class);

    private static final String bootstrapServer = "localhost:9092";

    private Properties propertiesProducer;

    private KafkaProducer<String, byte[]> kafkaProducer;

    private static final String TOPIC = "test";

    private MessageByteSerializer messageByteSerializer;

    @Override
    public void packetReceived(IBroadcastStream stream, IStreamPacket packet) {
        log.info("This is Kafka Source Listener");
        byte[] data = messageByteSerializer.encode(packet);

        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(TOPIC, data);
        kafkaProducer.send(record);
        kafkaProducer.flush();
    }

    public void init() {
        //init producer
        propertiesProducer = new Properties();
        propertiesProducer.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        propertiesProducer.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesProducer.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducer = new KafkaProducer<String, byte[]>(propertiesProducer);

        messageByteSerializer = new MessageByteSerializer();
    }
}
