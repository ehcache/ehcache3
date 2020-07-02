package org.ehcache.clustered.server.internal.messages;

import org.ehcache.clustered.common.internal.messages.EhcacheCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;

import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class ReconnectPassiveReplicationMessageCodec {
  private static final String SEQUENCE_ID = "sequence_id";
  private static final String SOURCE_ID = "client_source_id";
  private static final String TRANSACTION_ID = "transaction_id";
  private static final String REQUEST = "request";
  private static final String RESPONSE = "response";

  private static final Struct RECONNECT_PASSIVE_REPLICATION_MESSAGE_STRUCT = newStructBuilder()
    .int64(SEQUENCE_ID, 10)
    .int64(SOURCE_ID, 20)
    .int64(TRANSACTION_ID, 30)
    .byteBuffer(REQUEST, 40)
    .byteBuffer(RESPONSE, 50)
    .build();

  public byte[] encode(ReconnectPassiveReplicationMessage replicationMessage, EhcacheServerCodec ehcacheServerCodec) {
    final StructEncoder<Void> encoder = RECONNECT_PASSIVE_REPLICATION_MESSAGE_STRUCT.encoder();
    encoder.int64(SEQUENCE_ID, replicationMessage.getSequenceId());
    encoder.int64(SOURCE_ID, replicationMessage.getClientSourceId());
    encoder.int64(TRANSACTION_ID, replicationMessage.getTransactionId());
    encoder.byteBuffer(REQUEST, ByteBuffer.wrap(ehcacheServerCodec.encodeMessage(replicationMessage.getRequest())));
    try {
      encoder.byteBuffer(RESPONSE, ByteBuffer.wrap(ehcacheServerCodec.encodeResponse(replicationMessage.getResponse())));
    } catch (MessageCodecException e) {
      throw new RuntimeException(e);
    }

    return encoder.encode().array();
  }

  public ReconnectPassiveReplicationMessage decode(ByteBuffer byteBuffer, EhcacheServerCodec ehcacheServerCodec) {
    final StructDecoder<Void> decoder = RECONNECT_PASSIVE_REPLICATION_MESSAGE_STRUCT.decoder(byteBuffer);
    if (decoder != null) {
      Long sequenceId = decoder.int64(SEQUENCE_ID);
      Long clientSourceId = decoder.int64(SOURCE_ID);
      Long transactionId = decoder.int64(TRANSACTION_ID);
      EhcacheEntityMessage request = ehcacheServerCodec.decodeMessage(decoder.byteBuffer(REQUEST).array());
      EhcacheEntityResponse response = null;
      try {
        response = ehcacheServerCodec.decodeResponse(decoder.byteBuffer(RESPONSE).array());
      } catch (MessageCodecException e) {
        throw new RuntimeException(e);
      }

      return new ReconnectPassiveReplicationMessage(sequenceId, clientSourceId, transactionId, request, response);
    }

    return null;
  }
}
