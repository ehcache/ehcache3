package org.ehcache.clustered.server.internal.messages;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;

public class ReconnectPassiveReplicationMessage extends EhcacheOperationMessage {
  private final long sequenceId;
  private final long clientSourceId;
  private final long transactionId;
  private final EhcacheEntityMessage request;
  private final EhcacheEntityResponse response;

  public ReconnectPassiveReplicationMessage(long sequenceId,
                                            long clientSourceId,
                                            long transactionId,
                                            EhcacheEntityMessage request,
                                            EhcacheEntityResponse response) {
    this.sequenceId = sequenceId;
    this.clientSourceId = clientSourceId;
    this.transactionId = transactionId;
    this.request = request;
    this.response = response;
  }

  public long getSequenceId() {
    return sequenceId;
  }

  public long getClientSourceId() {
    return clientSourceId;
  }

  public long getTransactionId() {
    return transactionId;
  }

  public EhcacheEntityMessage getRequest() {
    return request;
  }

  public EhcacheEntityResponse getResponse() {
    return response;
  }

  @Override
  public EhcacheMessageType getMessageType() {
    return EhcacheMessageType.RECONNECT_PASSIVE_REPLICATION_MESSAGE;
  }
}
