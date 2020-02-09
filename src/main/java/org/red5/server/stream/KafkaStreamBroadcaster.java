package org.red5.server.stream;

import org.red5.server.messaging.IPipe;
import org.red5.server.messaging.PipeConnectionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * This class reads from Kafka and broadcasts to subscribers
 */
@ManagedResource(objectName = "org.red5.server:type=KafkaStreamBroadcaster", description = "KafkaStreamBroadcaster")
public class KafkaStreamBroadcaster extends ClientBroadcastStream {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamBroadcaster.class);

    protected transient IPipe cachedLivePipe = null;

    protected boolean broadcastFromKafka = true;

    protected String kafkaBrokerAddress;

    /**
     * @param kafkaBrokerAddress
     *            the kafkaBrokerAddress to set
     */
    public void setKafkaBrokerAddress(String kafkaBrokerAddress) {
        log.info("Setting kafka address {}", kafkaBrokerAddress);
        this.kafkaBrokerAddress = kafkaBrokerAddress;
    }

    /**
     * @param broadcastFromKafka
     *            the broadcastFromKafka to set
     */

    public void setBroadcastFromKafka(boolean broadcastFromKafka) {
        this.broadcastFromKafka = broadcastFromKafka;
    }

    public KafkaStreamBroadcaster() {
        super();
        init();
    }

    public void init() {
        //TODO: init the consumer and the thread to poll from Kafka and write to livePipe
    }

    @Override
    public void onPipeConnectionEvent(PipeConnectionEvent event) {
        super.onPipeConnectionEvent(event);

        // if we broadcast from Kafka stream, set the livePipe to null to disable
        // direct live broadcast within function dispatch
        if (broadcastFromKafka && livePipe != null) {
            cachedLivePipe = livePipe;
        }
    }

    @Override
    public void close() {
        if (broadcastFromKafka && cachedLivePipe != null) {
            livePipe = cachedLivePipe;
            cachedLivePipe = null;
        }
        super.close();
    }
}
