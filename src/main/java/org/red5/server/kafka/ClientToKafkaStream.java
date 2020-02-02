package org.red5.server.kafka;

import org.red5.server.stream.ClientBroadcastStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(objectName = "org.red5.server:type=ClientWriteToKafka", description = "ClientWriteToKafka")
public class ClientToKafkaStream extends ClientBroadcastStream {

    private static final Logger log = LoggerFactory.getLogger(ClientToKafkaStream.class);

    protected KafkaSourceListener kafkaSourceListener;

    protected String kafkaBrokerAddress;

    /**
     * @param kafkaBrokerAddress
     *            the kafkaBrokerAddress to set
     */
    public void setKafkaBrokerAddress(String kafkaBrokerAddress) {
        this.kafkaBrokerAddress = kafkaBrokerAddress;
    }

    @Override
    public void start() {
        log.info("Kafka broker address: {}", kafkaBrokerAddress);
        kafkaSourceListener = new KafkaSourceListener();

        addStreamListener(kafkaSourceListener);

        super.start();
    }

    @Override
    public void close() {
        removeStreamListener(kafkaSourceListener);
    }
}
