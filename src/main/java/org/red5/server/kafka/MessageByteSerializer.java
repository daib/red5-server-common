package org.red5.server.kafka;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.mina.core.buffer.IoBuffer;
import org.red5.server.api.stream.IStreamPacket;
import org.red5.server.kafka.KafkaProto.KafkaRTMPMessage;
import org.red5.server.net.rtmp.event.*;
import org.red5.server.net.rtmp.message.Constants;
import org.red5.server.stream.message.RTMPMessage;

public class MessageByteSerializer {
    public byte[] encode(IStreamPacket packet) {
        IoBuffer buf = packet.getData();
        byte[] arr = new byte[buf.capacity()];
        buf.clear();
        buf.get(arr, 0, buf.capacity());
        ByteString bufByteString = ByteString.copyFrom(arr);

        int timestamp = packet.getTimestamp();

        int datatyte = packet.getDataType();

        KafkaProto.KafkaRTMPMessage kafkaRTMPMessage = KafkaProto.KafkaRTMPMessage.newBuilder().setBuf(bufByteString).setDatatype(datatyte).setTimestamp(timestamp).build();

        byte[] data = kafkaRTMPMessage.toByteArray();
        return data;
    }

    public RTMPMessage decode(KafkaRTMPMessage kafkaRTMPMessage) {
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