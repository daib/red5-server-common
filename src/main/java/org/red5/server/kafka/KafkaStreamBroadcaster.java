package org.red5.server.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.red5.server.messaging.IPipe;
import org.red5.server.messaging.PipeConnectionEvent;
import org.red5.server.stream.*;
import org.red5.server.stream.message.RTMPMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.red5.server.kafka.KafkaProto.KafkaRTMPMessage;

import java.io.IOException;
import java.util.*;

/**
 * This class reads from Kafka and broadcasts to subscribers
 */
@ManagedResource(objectName = "org.red5.server:type=KafkaStreamBroadcaster", description = "KafkaStreamBroadcaster")
public class KafkaStreamBroadcaster extends ClientBroadcastStream {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamBroadcaster.class);

    protected String kafkaBrokerAddress;

    private static final String bootstrapServer = "localhost:9092";

    private Properties properties;

    private static final String TOPIC = "test";

    private MessageByteSerializer messageByteSerializer = new MessageByteSerializer();

    private boolean isCloseStream;

    private MyKafkaConsumer myKafkaConsumer;

    private KafkaThread kafkaThread;

    protected KafkaSourceListener kafkaSourceListener;

    protected transient IPipe myLivePipe;

    /**
     * @param kafkaBrokerAddress
     *            the kafkaBrokerAddress to set
     */
    public void setKafkaBrokerAddress(String kafkaBrokerAddress) {
        this.kafkaBrokerAddress = kafkaBrokerAddress;
    }

    public KafkaStreamBroadcaster() {
        super();
        init();
        log.info("this is KafkaStreamBroadcaster");
        isCloseStream = false;
        log.info("kafkaBrokerAddress   " + kafkaBrokerAddress);
    }

    public void init() {
        log.info("this is init kafkastreambroadcaster");
        //TODO: init the consumer and the thread to poll from Kafka and write to livePipe

        myKafkaConsumer = new MyKafkaConsumer();
        myKafkaConsumer.init();
        kafkaThread = new KafkaThread();
        kafkaThread.start();
    }

    @Override
    public void close() {
        super.close();
        myKafkaConsumer.close();
        isCloseStream = true;
        kafkaSourceListener.close();
        removeStreamListener(kafkaSourceListener);
    }

    @Override
    public void start() {
        log.info("Kafka broker address: {}", kafkaBrokerAddress);
        super.start();
        kafkaSourceListener = new KafkaSourceListener();
        kafkaSourceListener.init(kafkaBrokerAddress);
        kafkaSourceListener.createTopic(this.publishedName);

        addStreamListener(kafkaSourceListener);

    }

    @Override
    public void onPipeConnectionEvent(PipeConnectionEvent event) {
        super.onPipeConnectionEvent(event);
        myLivePipe = livePipe;
        livePipe = null;
    }

    //KafkaThread class
    private class KafkaThread extends Thread {
        public void run() {
            //run thread check livePipe
            while (!isCloseStream) {
                //log.info("KafkaThread");
                if (livePipe != null) {
                    //Creae RTMPMmessage
                    ConsumerRecords<String, byte[]> records = myKafkaConsumer.receive();
                    records.forEach(record -> {
                        byte[] data = record.value();
                        try {
                            KafkaRTMPMessage kafkaRTMPMessage = KafkaRTMPMessage.parseFrom(data);
                            RTMPMessage msg = messageByteSerializer.decode(kafkaRTMPMessage);
                            livePipe.pushMessage(msg);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                            stop();
                        }
                    });
                } else {
                    log.debug("Live pipe was null, message was not pushed");
                }
            }
        }
    }

}