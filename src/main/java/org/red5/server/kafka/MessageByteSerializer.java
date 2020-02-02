package org.red5.server.kafka;

import com.google.protobuf.ByteString;
import org.apache.mina.core.buffer.IoBuffer;
import org.red5.server.api.stream.IStreamPacket;

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
}
