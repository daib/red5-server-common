package org.red5.server.stream;

import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.api.stream.IStreamListener;
import org.red5.server.api.stream.IStreamPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceListener implements IStreamListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceListener.class);

    @Override
    public void packetReceived(IBroadcastStream stream, IStreamPacket packet) {
        log.info("This is Kafka Source Listener");
    }
}
