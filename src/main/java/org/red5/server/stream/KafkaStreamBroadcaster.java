package org.red5.server.stream;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.red5.server.kafka.KafkaConsumerWrapper;
import org.red5.server.kafka.KafkaProto.KafkaRTMPMessage;
import org.red5.server.kafka.MessageByteConverter;
import org.red5.server.messaging.IPipe;
import org.red5.server.messaging.PipeConnectionEvent;
import org.red5.server.stream.message.RTMPMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * This class reads from Kafka and broadcasts to subscribers
 */
@ManagedResource(objectName = "org.red5.server:type=KafkaStreamBroadcaster", description = "KafkaStreamBroadcaster")
public class KafkaStreamBroadcaster extends ClientToKafkaStream {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamBroadcaster.class);

    protected transient IPipe cachedLivePipe = null;

    protected boolean broadcastFromKafka = true;

    private boolean isStreamClosed = true;

    private KafkaConsumerWrapper kafkaConsumer = null;

    KafkaPollingThread kafkaThread = null;

    /**
     * @param broadcastFromKafka
     *            the broadcastFromKafka to set
     */

    public void setBroadcastFromKafka(boolean broadcastFromKafka) {
        log.info("Broadcasting from Kafka: {}", broadcastFromKafka);
        this.broadcastFromKafka = broadcastFromKafka;
    }

    public void init(String bootstrapServer, String topic) {
        log.info("Initializing {}", this.getClass());
        kafkaConsumer = new KafkaConsumerWrapper();
        kafkaConsumer.init(bootstrapServer, topic, topic);

        isStreamClosed = false;

        kafkaThread = new KafkaPollingThread();
        kafkaThread.start();
    }

    @Override
    public void onPipeConnectionEvent(PipeConnectionEvent event) {
        super.onPipeConnectionEvent(event);

        // if we broadcast from Kafka stream, set the livePipe to null to disable
        // direct live broadcast within function dispatch
        if (broadcastFromKafka && livePipe != null) {
            cachedLivePipe = livePipe;
            livePipe = null;
        }
    }

    @Override
    public void close() {
        if (broadcastFromKafka && cachedLivePipe != null) {
            livePipe = cachedLivePipe;
            cachedLivePipe = null;

            kafkaConsumer.close();
            kafkaConsumer = null;
        }

        isStreamClosed = true;

        super.close();
    }

    @Override
    public void setPublishedName(String name) {
        super.setPublishedName(name);

        if (broadcastFromKafka) {
            init(kafkaBrokerAddress, name);
        }
    }

    //KafkaThread class
    private class KafkaPollingThread extends Thread {
        public void run() {
            //run thread check livePipe
            while (!isStreamClosed) {
                if (cachedLivePipe != null) {
                    //Create RTMPMmessage
                    ConsumerRecords<String, byte[]> records = kafkaConsumer.receive();
                    records.forEach(record -> {
                        RTMPMessage msg = MessageByteConverter.decode(record.value());

                        if (msg != null) {
                            try {
                                cachedLivePipe.pushMessage(msg);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                } else {
                    log.debug("Live pipe was null, message was not pushed");
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
