package org.red5.server.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class reads from Kafka and broadcasts to subscribers
 */
public class KafkaStreamBroadcaster extends ClientBroadcastStream {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamBroadcaster.class);

    protected String kafkaBrokerAddress;

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
    }

    public void init() {
        //TODO: init the consumer and the thread to poll from Kafka and write to livePipe
    }

}
