package org.ehcache.clustered.common.messages;

import org.ehcache.clustered.common.store.Chain;

import java.nio.ByteBuffer;

public class ServerStoreMessageFactory {

  private final String cacheId;

  public ServerStoreMessageFactory(String cacheId) {
    this.cacheId = cacheId;
  }

  public EhcacheEntityMessage getOperation(long key) {
    return new ServerStoreOpMessage.GetMessage(this.cacheId, key);
  }

  public EhcacheEntityMessage getAndAppendOperation(long key, ByteBuffer payload) {
    return new ServerStoreOpMessage.GetAndAppendMessage(this.cacheId, key, payload);
  }

  public EhcacheEntityMessage appendOperation(long key, ByteBuffer payload) {
    return new ServerStoreOpMessage.AppendMessage(this.cacheId, key, payload);
  }

  public EhcacheEntityMessage replaceAtHeadOperation(long key, Chain expect, Chain update) {
    return new ServerStoreOpMessage.ReplaceAtHeadMessage(this.cacheId, key, expect, update);
  }
}

