package org.red5.server.kafka;

import org.apache.mina.core.buffer.IoBuffer;
import org.red5.codec.IAudioStreamCodec;
import org.red5.codec.IStreamCodecInfo;
import org.red5.codec.IVideoStreamCodec;
import org.red5.codec.StreamCodecInfo;
import org.red5.io.amf.Output;
import org.red5.server.api.IConnection;
import org.red5.server.api.event.IEvent;
import org.red5.server.api.event.IEventListener;
import org.red5.server.api.scope.IScope;
import org.red5.server.api.stream.IStreamAwareScopeHandler;
import org.red5.server.api.stream.IStreamListener;
import org.red5.server.api.stream.IStreamPacket;
import org.red5.server.api.stream.StreamState;
import org.red5.server.net.rtmp.event.*;
import org.red5.server.net.rtmp.message.Constants;
import org.red5.server.net.rtmp.message.Header;
import org.red5.server.net.rtmp.status.Status;
import org.red5.server.net.rtmp.status.StatusCodes;
import org.red5.server.stream.*;
import org.red5.server.stream.message.RTMPMessage;
import org.red5.server.stream.message.StatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

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
        super.start();
        kafkaSourceListener = new KafkaSourceListener();
        kafkaSourceListener.init("localhost:9092");
        addStreamListener(kafkaSourceListener);

    }

    @Override
    public void close() {
        super.close();
        kafkaSourceListener.close();
        removeStreamListener(kafkaSourceListener);
    }

}
