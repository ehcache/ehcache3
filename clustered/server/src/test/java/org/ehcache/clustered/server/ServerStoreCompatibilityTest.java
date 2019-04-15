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

package org.ehcache.clustered.server;

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.PoolAllocation.Dedicated;
import org.ehcache.clustered.common.PoolAllocation.Shared;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerStoreConfigurationException;
import org.ehcache.clustered.common.PoolAllocation.Unknown;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *
 * @author GGIB
 */
public class ServerStoreCompatibilityTest {

  private static final String ERROR_MESSAGE_BASE = "Existing ServerStore configuration is not compatible with the desired configuration: " + "\n\t";
  private static final PoolAllocation DEDICATED_POOL_ALLOCATION = new Dedicated("primary",4);
  private static final PoolAllocation SHARED_POOL_ALLOCATION = new Shared("sharedPool");
  private static final PoolAllocation UNKNOWN_POOL_ALLOCATION = new Unknown();
  private static final String STORED_KEY_TYPE = Long.class.getName();
  private static final String STORED_VALUE_TYPE = String.class.getName();
  private static final String KEY_SERIALIZER_TYPE = Long.class.getName();
  private static final String VALUE_SERIALIZER_TYPE = String.class.getName();

  @Test
  public void testStoredKeyTypeMismatch() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                String.class.getName(),
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "storedKeyType existing: " + serverConfiguration.getStoredKeyType() + ", desired: " + clientConfiguration.getStoredKeyType()),is(true));
    }
  }

  @Test
  public void testStoredValueTypeMismatch() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                Long.class.getName(),
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "storedValueType existing: " + serverConfiguration.getStoredValueType() + ", desired: " + clientConfiguration.getStoredValueType()), is(true));
    }
  }

  @Test
  public void testKeySerializerTypeMismatch() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                Double.class.getName(),
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "keySerializerType existing: " + serverConfiguration.getKeySerializerType() + ", desired: " + clientConfiguration.getKeySerializerType()),is(true));
    }
  }

  @Test
  public void testValueSerializerTypeMismatch() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                Double.class.getName(),
                                                                                Consistency.EVENTUAL, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "valueSerializerType existing: " + serverConfiguration.getValueSerializerType() + ", desired: " + clientConfiguration.getValueSerializerType()),is(true));
    }
  }

  @Test
  public void testConsitencyMismatch() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "consistencyType existing: " + serverConfiguration.getConsistency() + ", desired: " + clientConfiguration.getConsistency()),is(true));
    }
  }

  @Test
  public void testDedicatedPoolResourceTooBig() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(new Dedicated("primary",8),
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat(e.getMessage(), containsString("resourcePoolType"));
    }
  }

  @Test
  public void testDedicatedPoolResourceTooSmall() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(new Dedicated("primary",2),
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat(e.getMessage(), containsString("resourcePoolType"));
    }
  }

  @Test
  public void testDedicatedPoolResourceNameMismatch() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(new Dedicated("primaryBad",4),
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat(e.getMessage(), containsString("resourcePoolType"));
    }
  }

  @Test
  public void testSharedPoolResourceNameMismatch() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(SHARED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(new Shared("sharedPoolBad"),
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat(e.getMessage(), containsString("resourcePoolType"));
    }
  }

  @Test
  public void testAllResourceParametersMatch() throws Exception
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
  }

  @Test
  public void testPoolResourceTypeMismatch() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(SHARED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat(e.getMessage(), containsString("resourcePoolType"));
    }
  }

  @Test
  public void testClientStoreConfigurationUnknownPoolResource() throws InvalidServerStoreConfigurationException {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(UNKNOWN_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);

  }

  @Test
  public void testServerStoreConfigurationUnknownPoolResourceInvalidKeyType() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(UNKNOWN_POOL_ALLOCATION,
                                                                                String.class.getName(),
                                                                                STORED_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "storedKeyType existing: " + serverConfiguration.getStoredKeyType() + ", desired: " + clientConfiguration.getStoredKeyType()),is(true));
    }

  }

  @Test
  public void testServerStoreConfigurationExtendedPoolAllocationType() {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(DEDICATED_POOL_ALLOCATION,
      STORED_KEY_TYPE,
      STORED_VALUE_TYPE,
      KEY_SERIALIZER_TYPE,
      VALUE_SERIALIZER_TYPE,
      Consistency.STRONG, false);

    PoolAllocation extendedPoolAllocation = new PoolAllocation.DedicatedPoolAllocation() {

      private static final long serialVersionUID = 1L;

      @Override
      public long getSize() {
        return 4;
      }

      @Override
      public String getResourceName() {
        return "primary";
      }

      @Override
      public boolean isCompatible(final PoolAllocation other) {
        return true;
      }
    };

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(extendedPoolAllocation,
      STORED_KEY_TYPE,
      STORED_VALUE_TYPE,
      KEY_SERIALIZER_TYPE,
      VALUE_SERIALIZER_TYPE,
      Consistency.STRONG, false);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected InvalidServerStoreConfigurationException");
    } catch(InvalidServerStoreConfigurationException e) {
      assertThat(e.getMessage(), containsString("resourcePoolType"));
    }
  }
}
