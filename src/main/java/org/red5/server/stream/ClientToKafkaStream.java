package org.red5.server.stream;

import org.red5.server.kafka.KafkaSourceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(objectName = "org.red5.server:type=ClientToKafkaStream", description = "ClientToKafkaStream")
public class ClientToKafkaStream extends ClientBroadcastStream {

    private static final Logger log = LoggerFactory.getLogger(ClientToKafkaStream.class);

    protected KafkaSourceListener kafkaSourceListener = null;

    protected boolean writeToKafka = true;

    protected String kafkaBrokerAddress;

    public void setWriteToKafka(boolean writeToKafka) {
        log.info("Write stream to Kafka: {}", writeToKafka);
        this.writeToKafka = writeToKafka;
    }

    /**
     * @param kafkaBrokerAddress
     *            the kafkaBrokerAddress to set
     */
    public void setKafkaBrokerAddress(String kafkaBrokerAddress) {
        log.info("Bootstraper server: {}", kafkaBrokerAddress);
        this.kafkaBrokerAddress = kafkaBrokerAddress;
    }

    @Override
    public void start() {
        if (writeToKafka) {
            log.info("Kafka broker address: {}", kafkaBrokerAddress);
            kafkaSourceListener = new KafkaSourceListener();

            kafkaSourceListener.init(kafkaBrokerAddress);

            addStreamListener(kafkaSourceListener);
        }

        super.start();
    }

    @Override
    public void close() {
        super.close();

        if (kafkaSourceListener != null) {
            kafkaSourceListener.close();
            removeStreamListener(kafkaSourceListener);
            kafkaSourceListener = null;
        }
    }

    @Override
    public void setPublishedName(String name) {
        super.setPublishedName(name);

        if (kafkaSourceListener != null) {
            kafkaSourceListener.createTopic(getPublishedName());
        }
    }
}
