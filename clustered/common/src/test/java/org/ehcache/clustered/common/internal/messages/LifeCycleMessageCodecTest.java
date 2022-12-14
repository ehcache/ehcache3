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

package org.ehcache.clustered.common.internal.messages;

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static java.nio.ByteBuffer.wrap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * LifeCycleMessageCodecTest
 */
public class LifeCycleMessageCodecTest {

  private static final long MESSAGE_ID = 42L;

  private final LifeCycleMessageFactory factory = new LifeCycleMessageFactory();
  private final LifeCycleMessageCodec codec = new LifeCycleMessageCodec(new CommonConfigCodec());

  @Before
  public void setUp() {
  }

  @Test
  public void testValidateStoreManager() throws Exception {
    ServerSideConfiguration configuration = getServerSideConfiguration();
    LifecycleMessage message = factory.validateStoreManager(configuration);

    byte[] encoded = codec.encode(message);
    LifecycleMessage.ValidateStoreManager decodedMessage = (LifecycleMessage.ValidateStoreManager) codec.decode(message.getMessageType(), wrap(encoded));

    assertThat(decodedMessage.getMessageType(), is(EhcacheMessageType.VALIDATE));
    assertThat(decodedMessage.getConfiguration().getDefaultServerResource(), is(configuration.getDefaultServerResource()));
    assertThat(decodedMessage.getConfiguration().getResourcePools(), is(configuration.getResourcePools()));
  }

  @Test
  public void testValidateServerStoreDedicated() throws Exception {
    PoolAllocation.Dedicated dedicated = new PoolAllocation.Dedicated("dedicate", 420000L);
    ServerStoreConfiguration configuration = new ServerStoreConfiguration(dedicated, "java.lang.Long", "java.lang.String",
      "org.ehcache.impl.serialization.LongSerializer", "org.ehcache.impl.serialization.StringSerializer",
      Consistency.STRONG, false);
    LifecycleMessage message = factory.validateServerStore("store1", configuration);

    byte[] encoded = codec.encode(message);
    LifecycleMessage.ValidateServerStore decodedMessage = (LifecycleMessage.ValidateServerStore) codec.decode(message.getMessageType(), wrap(encoded));

    assertThat(decodedMessage.getMessageType(), is(EhcacheMessageType.VALIDATE_SERVER_STORE));
    validateCommonServerStoreConfig(decodedMessage, configuration);
    PoolAllocation.Dedicated decodedPoolAllocation = (PoolAllocation.Dedicated) decodedMessage.getStoreConfiguration().getPoolAllocation();
    assertThat(decodedPoolAllocation.getResourceName(), is(dedicated.getResourceName()));
    assertThat(decodedPoolAllocation.getSize(), is(dedicated.getSize()));
    assertThat(decodedMessage.getStoreConfiguration().isLoaderWriterConfigured(), is(false));
  }

  @Test
  public void testValidateServerStoreShared() throws Exception {
    PoolAllocation.Shared shared = new PoolAllocation.Shared("shared");
    ServerStoreConfiguration configuration = new ServerStoreConfiguration(shared, "java.lang.Long", "java.lang.String",
      "org.ehcache.impl.serialization.LongSerializer", "org.ehcache.impl.serialization.StringSerializer",
      Consistency.STRONG, false);
    LifecycleMessage message = factory.validateServerStore("store1", configuration);

    byte[] encoded = codec.encode(message);
    LifecycleMessage.ValidateServerStore decodedMessage = (LifecycleMessage.ValidateServerStore) codec.decode(message.getMessageType(), wrap(encoded));

    assertThat(decodedMessage.getMessageType(), is(EhcacheMessageType.VALIDATE_SERVER_STORE));
    validateCommonServerStoreConfig(decodedMessage, configuration);
    PoolAllocation.Shared decodedPoolAllocation = (PoolAllocation.Shared) decodedMessage.getStoreConfiguration().getPoolAllocation();
    assertThat(decodedPoolAllocation.getResourcePoolName(), is(shared.getResourcePoolName()));
    assertThat(decodedMessage.getStoreConfiguration().isLoaderWriterConfigured(), is(false));
  }

  @Test
  public void testValidateServerStoreUnknown() throws Exception {
    PoolAllocation.Unknown unknown = new PoolAllocation.Unknown();
    ServerStoreConfiguration configuration = new ServerStoreConfiguration(unknown, "java.lang.Long", "java.lang.String",
      "org.ehcache.impl.serialization.LongSerializer", "org.ehcache.impl.serialization.StringSerializer",
      Consistency.STRONG, false);
    LifecycleMessage message = factory.validateServerStore("store1", configuration);

    byte[] encoded = codec.encode(message);
    LifecycleMessage.ValidateServerStore decodedMessage = (LifecycleMessage.ValidateServerStore) codec.decode(message.getMessageType(), wrap(encoded));

    assertThat(decodedMessage.getMessageType(), is(EhcacheMessageType.VALIDATE_SERVER_STORE));
    validateCommonServerStoreConfig(decodedMessage, configuration);
    assertThat(decodedMessage.getStoreConfiguration().getPoolAllocation(), instanceOf(PoolAllocation.Unknown.class));
    assertThat(decodedMessage.getStoreConfiguration().isLoaderWriterConfigured(), is(false));
  }

  private void validateCommonServerStoreConfig(LifecycleMessage.ValidateServerStore decodedMessage, ServerStoreConfiguration initialConfiguration) {
    assertThat(decodedMessage.getName(), is("store1"));
    assertThat(decodedMessage.getStoreConfiguration().getStoredKeyType(), is(initialConfiguration.getStoredKeyType()));
    assertThat(decodedMessage.getStoreConfiguration().getStoredValueType(), is(initialConfiguration.getStoredValueType()));
    assertThat(decodedMessage.getStoreConfiguration().getConsistency(), is(initialConfiguration.getConsistency()));
    assertThat(decodedMessage.getStoreConfiguration().getKeySerializerType(), is(initialConfiguration.getKeySerializerType()));
    assertThat(decodedMessage.getStoreConfiguration().getValueSerializerType(), is(initialConfiguration.getValueSerializerType()));
  }

  private ServerSideConfiguration getServerSideConfiguration() {
    return new ServerSideConfiguration("default", Collections.singletonMap("shared", new ServerSideConfiguration.Pool(100, "other")));
  }

}
