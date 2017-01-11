/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.clustered.server.internal.messages;

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.CommonConfigCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ChainReplicationMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ClearInvalidationCompleteMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ClientIDTrackerMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.junit.Test;

import java.util.UUID;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.store.Util.chainsEqual;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class PassiveReplicationMessageCodecTest {

  private static final long MESSAGE_ID = 42L;
  private PassiveReplicationMessageCodec codec = new PassiveReplicationMessageCodec(new CommonConfigCodec());

  @Test
  public void testClientIDTrackerMessageCodec() {
    ClientIDTrackerMessage clientIDTrackerMessage = new ClientIDTrackerMessage(UUID.randomUUID());

    byte[] encoded = codec.encode(clientIDTrackerMessage);
    PassiveReplicationMessage decodedMsg = (PassiveReplicationMessage) codec.decode(EhcacheMessageType.CLIENT_ID_TRACK_OP, wrap(encoded));

    assertThat(decodedMsg.getClientId(), is(clientIDTrackerMessage.getClientId()));

  }

  @Test
  public void testChainReplicationMessageCodec() {
    Chain chain = getChain(false, createPayload(2L), createPayload(20L));
    ChainReplicationMessage chainReplicationMessage = new ChainReplicationMessage("test", 2L, chain, 200L, UUID.randomUUID());

    byte[] encoded = codec.encode(chainReplicationMessage);
    ChainReplicationMessage decodedMsg = (ChainReplicationMessage) codec.decode(EhcacheMessageType.CHAIN_REPLICATION_OP, wrap(encoded));

    assertThat(decodedMsg.getCacheId(), is(chainReplicationMessage.getCacheId()));
    assertThat(decodedMsg.getClientId(), is(chainReplicationMessage.getClientId()));
    assertThat(decodedMsg.getId(), is(chainReplicationMessage.getId()));
    assertThat(decodedMsg.getKey(), is(chainReplicationMessage.getKey()));
    assertTrue(chainsEqual(decodedMsg.getChain(), chainReplicationMessage.getChain()));

  }

  @Test
  public void testClearInvalidationCompleteMessage() {
    ClearInvalidationCompleteMessage clearInvalidationCompleteMessage = new ClearInvalidationCompleteMessage("test");

    byte[] encoded = codec.encode(clearInvalidationCompleteMessage);
    ClearInvalidationCompleteMessage decoded = (ClearInvalidationCompleteMessage) codec.decode(EhcacheMessageType.CLEAR_INVALIDATION_COMPLETE, wrap(encoded));

    assertThat(decoded.getMessageType(), is(EhcacheMessageType.CLEAR_INVALIDATION_COMPLETE));
    assertThat(decoded.getCacheId(), is(clearInvalidationCompleteMessage.getCacheId()));

  }

  @Test
  public void testInvalidationCompleteMessage() {

    InvalidationCompleteMessage invalidationCompleteMessage = new InvalidationCompleteMessage("test", 20L);

    byte[] encoded = codec.encode(invalidationCompleteMessage);
    InvalidationCompleteMessage decoded = (InvalidationCompleteMessage) codec.decode(EhcacheMessageType.INVALIDATION_COMPLETE, wrap(encoded));

    assertThat(decoded.getMessageType(), is(EhcacheMessageType.INVALIDATION_COMPLETE));
    assertThat(decoded.getCacheId(), equalTo(invalidationCompleteMessage.getCacheId()));
    assertThat(decoded.getKey(), equalTo(invalidationCompleteMessage.getKey()));
  }

  @Test
  public void testCreateServerStoreReplicationDedicated() throws Exception {
    UUID clientId = UUID.randomUUID();
    PoolAllocation.Dedicated dedicated = new PoolAllocation.Dedicated("dedicate", 420000L);
    ServerStoreConfiguration configuration = new ServerStoreConfiguration(dedicated, "java.lang.Long", "java.lang.String", null, null,
      "org.ehcache.impl.serialization.LongSerializer", "org.ehcache.impl.serialization.StringSerializer",
      Consistency.STRONG);
    PassiveReplicationMessage.CreateServerStoreReplicationMessage message = new PassiveReplicationMessage.CreateServerStoreReplicationMessage(MESSAGE_ID, clientId, "storeId", configuration);

    byte[] encoded = codec.encode(message);
    PassiveReplicationMessage.CreateServerStoreReplicationMessage decodedMessage = (PassiveReplicationMessage.CreateServerStoreReplicationMessage) codec.decode(message.getMessageType(), wrap(encoded));

    assertThat(decodedMessage.getMessageType(), is(EhcacheMessageType.CREATE_SERVER_STORE_REPLICATION));
    assertThat(decodedMessage.getId(), is(MESSAGE_ID));
    assertThat(decodedMessage.getClientId(), is(clientId));
    assertThat(decodedMessage.getStoreName(), is("storeId"));
    assertThat(decodedMessage.getStoreConfiguration().getStoredKeyType(), is(configuration.getStoredKeyType()));
    assertThat(decodedMessage.getStoreConfiguration().getStoredValueType(), is(configuration.getStoredValueType()));
    assertThat(decodedMessage.getStoreConfiguration().getActualKeyType(), nullValue());
    assertThat(decodedMessage.getStoreConfiguration().getActualValueType(), nullValue());
    assertThat(decodedMessage.getStoreConfiguration().getConsistency(), is(configuration.getConsistency()));
    assertThat(decodedMessage.getStoreConfiguration().getKeySerializerType(), is(configuration.getKeySerializerType()));
    assertThat(decodedMessage.getStoreConfiguration().getValueSerializerType(), is(configuration.getValueSerializerType()));
    PoolAllocation.Dedicated decodedPoolAllocation = (PoolAllocation.Dedicated) decodedMessage.getStoreConfiguration().getPoolAllocation();
    assertThat(decodedPoolAllocation.getResourceName(), is(dedicated.getResourceName()));
    assertThat(decodedPoolAllocation.getSize(), is(dedicated.getSize()));
  }

  @Test
  public void testCreateServerStoreReplicationShared() throws Exception {
    UUID clientId = UUID.randomUUID();
    PoolAllocation.Shared shared = new PoolAllocation.Shared("shared");
    ServerStoreConfiguration configuration = new ServerStoreConfiguration(shared, "java.lang.Long", "java.lang.String", null, null,
      "org.ehcache.impl.serialization.LongSerializer", "org.ehcache.impl.serialization.StringSerializer",
      Consistency.STRONG);
    PassiveReplicationMessage.CreateServerStoreReplicationMessage message = new PassiveReplicationMessage.CreateServerStoreReplicationMessage(MESSAGE_ID, clientId, "storeId", configuration);

    byte[] encoded = codec.encode(message);
    PassiveReplicationMessage.CreateServerStoreReplicationMessage decodedMessage = (PassiveReplicationMessage.CreateServerStoreReplicationMessage) codec.decode(message.getMessageType(), wrap(encoded));

    assertThat(decodedMessage.getMessageType(), is(EhcacheMessageType.CREATE_SERVER_STORE_REPLICATION));
    assertThat(decodedMessage.getId(), is(MESSAGE_ID));
    assertThat(decodedMessage.getClientId(), is(clientId));
    assertThat(decodedMessage.getStoreName(), is("storeId"));
    assertThat(decodedMessage.getStoreConfiguration().getStoredKeyType(), is(configuration.getStoredKeyType()));
    assertThat(decodedMessage.getStoreConfiguration().getStoredValueType(), is(configuration.getStoredValueType()));
    assertThat(decodedMessage.getStoreConfiguration().getActualKeyType(), nullValue());
    assertThat(decodedMessage.getStoreConfiguration().getActualValueType(), nullValue());
    assertThat(decodedMessage.getStoreConfiguration().getConsistency(), is(configuration.getConsistency()));
    assertThat(decodedMessage.getStoreConfiguration().getKeySerializerType(), is(configuration.getKeySerializerType()));
    assertThat(decodedMessage.getStoreConfiguration().getValueSerializerType(), is(configuration.getValueSerializerType()));
    PoolAllocation.Shared decodedPoolAllocation = (PoolAllocation.Shared) decodedMessage.getStoreConfiguration().getPoolAllocation();
    assertThat(decodedPoolAllocation.getResourcePoolName(), is(shared.getResourcePoolName()));
  }

  @Test
  public void testDestroyServerStoreReplication() throws Exception {
    UUID clientId = UUID.randomUUID();
    PassiveReplicationMessage.DestroyServerStoreReplicationMessage message = new PassiveReplicationMessage.DestroyServerStoreReplicationMessage(MESSAGE_ID, clientId, "storeId");

    byte[] encoded = codec.encode(message);
    PassiveReplicationMessage.DestroyServerStoreReplicationMessage decodedMessage = (PassiveReplicationMessage.DestroyServerStoreReplicationMessage) codec.decode(message.getMessageType(), wrap(encoded));

    assertThat(decodedMessage.getMessageType(), is(EhcacheMessageType.DESTROY_SERVER_STORE_REPLICATION));
    assertThat(decodedMessage.getId(), is(MESSAGE_ID));
    assertThat(decodedMessage.getClientId(), is(clientId));
    assertThat(decodedMessage.getStoreName(), is("storeId"));
  }

}
