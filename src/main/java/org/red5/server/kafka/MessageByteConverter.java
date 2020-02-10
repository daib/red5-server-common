package org.red5.server.kafka;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mina.core.buffer.IoBuffer;
import org.red5.server.api.stream.IStreamPacket;
import org.red5.server.kafka.KafkaProto.KafkaRTMPMessage;
import org.red5.server.net.rtmp.event.*;
import org.red5.server.net.rtmp.message.Constants;
import org.red5.server.stream.message.RTMPMessage;

public class MessageByteConverter {
    public static byte[] encode(IStreamPacket packet) {
        ByteString bufByteString = ByteString.copyFrom(packet.getData().buf());

        int timestamp = packet.getTimestamp();

        int dataType = packet.getDataType();

        KafkaProto.KafkaRTMPMessage kafkaRTMPMessage = KafkaProto.KafkaRTMPMessage.newBuilder().setBuf(bufByteString).setDatatype(dataType).setTimestamp(timestamp).build();

        return kafkaRTMPMessage.toByteArray();
    }

    public static RTMPMessage decode(byte[] data) {
        KafkaRTMPMessage kafkaRTMPMessage;
        try {
            kafkaRTMPMessage = KafkaRTMPMessage.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return null;
        }
        IRTMPEvent event;
        RTMPMessage message = null;

        byte dataType = (byte) kafkaRTMPMessage.getDatatype();
        int timestamp = kafkaRTMPMessage.getTimestamp();
        IoBuffer buffer = IoBuffer.wrap(kafkaRTMPMessage.getBuf().toByteArray());
        int bufLimit = buffer.limit();
        if (bufLimit > 0) {
            switch (dataType) {
                case Constants.TYPE_AGGREGATE:
                    event = new Aggregate(buffer);
                    event.setTimestamp(timestamp);
                    message = RTMPMessage.build(event);
                    break;
                case Constants.TYPE_AUDIO_DATA:
                    event = new AudioData(buffer);
                    event.setTimestamp(timestamp);
                    message = RTMPMessage.build(event);
                    break;
                case Constants.TYPE_VIDEO_DATA:
                    event = new VideoData(buffer);
                    event.setTimestamp(timestamp);
                    message = RTMPMessage.build(event);
                    break;
                default:
                    event = new Notify(buffer);
                    event.setTimestamp(timestamp);
                    message = RTMPMessage.build(event);
                    break;
            }
        }
        return message;
    }

}