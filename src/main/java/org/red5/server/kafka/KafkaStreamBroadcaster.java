package org.red5.server.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.red5.server.messaging.IMessageOutput;
import org.red5.server.messaging.IPipe;
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
import org.red5.server.kafka.KafkaProto.KafkaRTMPMessage;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
        kafkaSourceListener.init();
        addStreamListener(kafkaSourceListener);

    }

    //KafkaThread class
    private class KafkaThread extends Thread {
        public void run() {
            //run thread check livePipe
            while (!isCloseStream) {
                log.info("KafkaThread");
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

    @Override
    public void dispatchEvent(IEvent event) {
        if (event instanceof IRTMPEvent && !closed.get()) {
            switch (event.getType()) {
                case STREAM_CONTROL:
                case STREAM_DATA:
                    // create the event
                    IRTMPEvent rtmpEvent;
                    try {
                        rtmpEvent = (IRTMPEvent) event;
                    } catch (ClassCastException e) {
                        log.error("Class cast exception in event dispatch", e);
                        return;
                    }
                    int eventTime = rtmpEvent.getTimestamp();
                    log.info("eventtime  " + eventTime);

                    // verify and / or set source type
                    if (rtmpEvent.getSourceType() != Constants.SOURCE_TYPE_LIVE) {
                        rtmpEvent.setSourceType(Constants.SOURCE_TYPE_LIVE);
                    }
                    /*
                     * if (log.isTraceEnabled()) { // If this is first packet save its timestamp; expect it is // absolute? no matter: it's never used! if (firstPacketTime == -1) { firstPacketTime =
                     * rtmpEvent.getTimestamp(); log.trace(String.format("CBS=@%08x: rtmpEvent=%s creation=%s firstPacketTime=%d", System.identityHashCode(this), rtmpEvent.getClass().getSimpleName(),
                     * creationTime, firstPacketTime)); } else { log.trace(String.format("CBS=@%08x: rtmpEvent=%s creation=%s firstPacketTime=%d timestamp=%d", System.identityHashCode(this),
                     * rtmpEvent.getClass().getSimpleName(), creationTime, firstPacketTime, eventTime)); } }
                     */
                    //get the buffer only once per call
                    IoBuffer buf = null;
                    if (rtmpEvent instanceof IStreamData && (buf = ((IStreamData<?>) rtmpEvent).getData()) != null) {
                        bytesReceived += buf.limit();
                    }
                    // get stream codec
                    IStreamCodecInfo codecInfo = getCodecInfo();
                    StreamCodecInfo info = null;
                    if (codecInfo instanceof StreamCodecInfo) {
                        info = (StreamCodecInfo) codecInfo;
                    }
                    //log.trace("Stream codec info: {}", info);
                    if (rtmpEvent instanceof AudioData) {
                        //log.trace("Audio: {}", eventTime);
                        IAudioStreamCodec audioStreamCodec = null;
                        if (checkAudioCodec) {
                            // dont try to read codec info from 0 length audio packets
                            if (buf.limit() > 0) {
                                audioStreamCodec = AudioCodecFactory.getAudioCodec(buf);
                                if (info != null) {
                                    info.setAudioCodec(audioStreamCodec);
                                }
                                checkAudioCodec = false;
                            }
                        } else if (codecInfo != null) {
                            audioStreamCodec = codecInfo.getAudioCodec();
                        }
                        if (audioStreamCodec != null) {
                            audioStreamCodec.addData(buf);
                        }
                        if (info != null) {
                            info.setHasAudio(true);
                        }
                    } else if (rtmpEvent instanceof VideoData) {
                        //log.trace("Video: {}", eventTime);
                        IVideoStreamCodec videoStreamCodec = null;
                        if (checkVideoCodec) {
                            videoStreamCodec = VideoCodecFactory.getVideoCodec(buf);
                            if (info != null) {
                                info.setVideoCodec(videoStreamCodec);
                            }
                            checkVideoCodec = false;
                        } else if (codecInfo != null) {
                            videoStreamCodec = codecInfo.getVideoCodec();
                        }
                        if (videoStreamCodec != null) {
                            videoStreamCodec.addData(buf, eventTime);
                        }
                        if (info != null) {
                            info.setHasVideo(true);
                        }
                    } else if (rtmpEvent instanceof Invoke) {
                        //Invoke invokeEvent = (Invoke) rtmpEvent;
                        //log.debug("Invoke action: {}", invokeEvent.getAction());
                        // event / stream listeners will not be notified of invokes
                        return;
                    } else if (rtmpEvent instanceof Notify) {
                        Notify notifyEvent = (Notify) rtmpEvent;
                        String action = notifyEvent.getAction();
                        //if (log.isDebugEnabled()) {
                        //log.debug("Notify action: {}", action);
                        //}
                        if ("onMetaData".equals(action)) {
                            // store the metadata
                            try {
                                //log.debug("Setting metadata");
                                setMetaData(notifyEvent.duplicate());
                            } catch (Exception e) {
                                log.warn("Metadata could not be duplicated for this stream", e);
                            }
                        }
                    }
                    // update last event time
                    if (eventTime > latestTimeStamp) {
                        latestTimeStamp = eventTime;
                    }
                    // notify event listeners
                    checkSendNotifications(event);

                    // notify listeners about received packet
                    if (rtmpEvent instanceof IStreamPacket) {
                        for (IStreamListener listener : getStreamListeners()) {
                            try {
                                listener.packetReceived(this, (IStreamPacket) rtmpEvent);
                            } catch (Exception e) {
                                log.error("Error while notifying listener {}", listener, e);
                                if (listener instanceof RecordingListener) {
                                    sendRecordFailedNotify(e.getMessage());
                                }
                            }
                        }
                    }
                    break;
                default:
                    // ignored event
                    //log.debug("Ignoring event: {}", event.getType());
            }
        } else {
            log.debug("Event was of wrong type or stream is closed ({})", closed);
        }
    }

    private void checkSendNotifications(IEvent event) {
        IEventListener source = event.getSource();
        sendStartNotifications(source);
    }

    private void sendStartNotifications(IEventListener source) {
        if (sendStartNotification) {
            // notify handler that stream starts recording/publishing
            sendStartNotification = false;
            if (source instanceof IConnection) {
                IScope scope = ((IConnection) source).getScope();
                if (scope.hasHandler()) {
                    final Object handler = scope.getHandler();
                    if (handler instanceof IStreamAwareScopeHandler) {
                        if (recordingListener != null && recordingListener.get().isRecording()) {
                            // callback for record start
                            ((IStreamAwareScopeHandler) handler).streamRecordStart(this);
                        } else {
                            // delete any previously recorded versions of this now "live" stream per
                            // http://livedocs.adobe.com/flashmediaserver/3.0/hpdocs/help.html?content=00000186.html
                            //                            try {
                            //                                File file = getRecordFile(scope, publishedName);
                            //                                if (file != null && file.exists()) {
                            //                                    if (!file.delete()) {
                            //                                        log.debug("File was not deleted: {}", file.getAbsoluteFile());
                            //                                    }
                            //                                }
                            //                            } catch (Exception e) {
                            //                                log.warn("Exception removing previously recorded file", e);
                            //                            }
                            // callback for publish start
                            ((IStreamAwareScopeHandler) handler).streamPublishStart(this);
                        }
                    }
                }
            }
            // send start notifications
            sendPublishStartNotify();
            if (recordingListener != null && recordingListener.get().isRecording()) {
                sendRecordStartNotify();
            }
            notifyBroadcastStart();
        }
    }

    /**
     * Sends record failed notifications
     */
    private void sendRecordFailedNotify(String reason) {
        Status failedStatus = new Status(StatusCodes.NS_RECORD_FAILED);
        failedStatus.setLevel(Status.ERROR);
        failedStatus.setClientid(getStreamId());
        failedStatus.setDetails(getPublishedName());
        failedStatus.setDesciption(reason);

        StatusMessage failedMsg = new StatusMessage();
        failedMsg.setBody(failedStatus);
        pushMessage(failedMsg);
    }

    /**
     * Sends publish start notifications
     */
    private void sendPublishStartNotify() {
        Status publishStatus = new Status(StatusCodes.NS_PUBLISH_START);
        publishStatus.setClientid(getStreamId());
        publishStatus.setDetails(getPublishedName());

        StatusMessage startMsg = new StatusMessage();
        startMsg.setBody(publishStatus);
        pushMessage(startMsg);
        setState(StreamState.PUBLISHING);
    }

    /**
     * Notifies handler on stream broadcast start
     */
    private void notifyBroadcastStart() {
        IStreamAwareScopeHandler handler = getStreamAwareHandler();
        if (handler != null) {
            try {
                handler.streamBroadcastStart(this);
            } catch (Throwable t) {
                log.error("Error in notifyBroadcastStart", t);
            }
        }
        // send metadata for creation and start dates
        IoBuffer buf = IoBuffer.allocate(256);
        buf.setAutoExpand(true);
        Output out = new Output(buf);
        out.writeString("onMetaData");
        Map<Object, Object> params = new HashMap<>();
        Calendar cal = GregorianCalendar.getInstance();
        cal.setTimeInMillis(creationTime);
        params.put("creationdate", ZonedDateTime.ofInstant(cal.toInstant(), ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT));
        cal.setTimeInMillis(startTime);
        params.put("startdate", ZonedDateTime.ofInstant(cal.toInstant(), ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT));
        if (log.isDebugEnabled()) {
            log.debug("Params: {}", params);
        }
        out.writeMap(params);
        buf.flip();
        Notify notify = new Notify(buf);
        notify.setAction("onMetaData");
        notify.setHeader(new Header());
        notify.getHeader().setDataType(Notify.TYPE_STREAM_METADATA);
        notify.getHeader().setStreamId(0);
        notify.setTimestamp(0);
        dispatchEvent(notify);
    }

    /**
     * Sends record start notifications
     */
    private void sendRecordStartNotify() {
        Status recordStatus = new Status(StatusCodes.NS_RECORD_START);
        recordStatus.setClientid(getStreamId());
        recordStatus.setDetails(getPublishedName());

        StatusMessage startMsg = new StatusMessage();
        startMsg.setBody(recordStatus);
        pushMessage(startMsg);
    }
}