package org.red5.server.kafka;

import com.google.protobuf.ByteString;
import org.apache.mina.core.buffer.IoBuffer;
import org.red5.server.api.stream.IStreamPacket;
import org.red5.server.kafka.KafkaProto.KafkaRTMPMessage;
import org.red5.server.net.rtmp.event.*;
import org.red5.server.net.rtmp.message.Constants;
import org.red5.server.stream.message.RTMPMessage;

public class MessageByteSerializer {
    public static byte[] encode(IStreamPacket packet) {
        IoBuffer buf = packet.getData();
        ByteString bufByteString = ByteString.copyFrom(buf.buf());

        int timestamp = packet.getTimestamp();

        int dataTyte = packet.getDataType();

        KafkaProto.KafkaRTMPMessage kafkaRTMPMessage = KafkaProto.KafkaRTMPMessage.newBuilder().setBuf(bufByteString).setDatatype(dataTyte).setTimestamp(timestamp).build();

        return kafkaRTMPMessage.toByteArray();
    }

    public static RTMPMessage decode(KafkaRTMPMessage kafkaRTMPMessage) {
        IRTMPEvent event = null;
        RTMPMessage message = null;

        byte dataType = (byte) kafkaRTMPMessage.getDatatype();
        int timestamp = kafkaRTMPMessage.getTimestamp();
        ByteString bufString = kafkaRTMPMessage.getBuf();
        byte[] data = bufString.toByteArray();
        IoBuffer buffer = IoBuffer.wrap(data);
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