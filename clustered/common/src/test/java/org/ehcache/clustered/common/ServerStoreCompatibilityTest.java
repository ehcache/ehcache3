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

package org.ehcache.clustered.common;

import junit.framework.Assert;
import org.ehcache.clustered.common.ServerStoreConfiguration.PoolAllocation;
import org.ehcache.clustered.common.ServerStoreConfiguration.PoolAllocation.Fixed;
import org.ehcache.clustered.common.ServerStoreConfiguration.PoolAllocation.Shared;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author GGIB
 */
public class ServerStoreCompatibilityTest
{

  private static final String ERROR_MESSAGE_BASE = "Existing ServerStore configuration is not compatible with the desired configuration: " + "\n\t";
  private static final PoolAllocation FIXED_POOL_ALLOCATION = new Fixed("primary",4);
  private static final PoolAllocation SHARED_POOL_ALLOCATION = new Shared("sharedPool");
  private static final String STORED_KEY_TYPE = Long.class.getName();
  private static final String STORED_VALUE_TYPE = String.class.getName();
  private static final String ACTUAL_KEY_TYPE = Long.class .getName();
  private static final String ACTUAL_VALUE_TYPE = String.class.getName();
  private static final String KEY_SERIALIZER_TYPE = Long.class.getName();
  private static final String VALUE_SERIALIZER_TYPE = String.class.getName();

  @Test
  public void testStoredKeyTypeMismatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                String.class.getName(),
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      System.out.println("errorMsg: " + e.getMessage());
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "storedKeyType existing: " + serverConfiguration.getStoredKeyType() + ", desired: " + clientConfiguration.getStoredKeyType()));
    }
  }

  @Test
  public void testStoredValueTypeMismatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                Long.class.getName(),
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "storedValueType existing: " + serverConfiguration.getStoredValueType() + ", desired: " + clientConfiguration.getStoredValueType()));
    }
  }

  @Test
  public void testStoredActualKeyTypeMismatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                String.class.getName(),
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "actualKeyType existing: " + serverConfiguration.getActualKeyType() + ", desired: " + clientConfiguration.getActualKeyType()));
    }
  }

  @Test
  public void testStoredActualValueTypeMismatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                Long.class.getName(),
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "actualValueType existing: " + serverConfiguration.getActualValueType() + ", desired: " + clientConfiguration.getActualValueType()));
    }
  }

  @Test
  public void testKeySerializerTypeMismatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                Double.class.getName(),
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "keySerializerType existing: " + serverConfiguration.getKeySerializerType() + ", desired: " + clientConfiguration.getKeySerializerType()));
    }
  }

  @Test
  public void testValueSerializerTypeMismatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                Double.class.getName(),
                                                                                Consistency.EVENTUAL);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "valueSerializerType existing: " + serverConfiguration.getValueSerializerType() + ", desired: " + clientConfiguration.getValueSerializerType()));
    }
  }

  @Test
  public void testConsitencyMismatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "consistencyType existing: " + serverConfiguration.getConsistency() + ", desired: " + clientConfiguration.getConsistency()));
    }
  }

  @Test
  public void testFixedPoolResourceTooBig()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(new Fixed("primary",8),
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "resourcePoolFixedSize existing: " + ((Fixed)serverConfiguration.getPoolAllocation()).getSize() + ", desired: " + ((Fixed)clientConfiguration.getPoolAllocation()).getSize()));
    }
  }

  @Test
  public void testFixedPoolResourceTooSmall()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(new Fixed("primary",2),
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "resourcePoolFixedSize existing: " + ((Fixed)serverConfiguration.getPoolAllocation()).getSize() + ", desired: " + ((Fixed)clientConfiguration.getPoolAllocation()).getSize()));
    }
  }

  @Test
  public void testFixedPoolResourceNameMismatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(new Fixed("primaryBad",4),
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "resourcePoolFixedResourceName existing: " + ((Fixed)serverConfiguration.getPoolAllocation()).getResourceName() + ", desired: " + ((Fixed)clientConfiguration.getPoolAllocation()).getResourceName()));
    }
  }

  @Test
  public void testSharedPoolResourceNameMismatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(SHARED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(new Shared("sharedPoolBad"),
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.STRONG);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
      fail("Expected ClusteredStoreValidationException");
    }
    catch(ClusteredStoreValidationException e)
    {
      Assert.assertTrue("test failed", e.getMessage().equals(ERROR_MESSAGE_BASE + "resourcePoolSharedPoolName existing: " + ((Shared)serverConfiguration.getPoolAllocation()).getResourcePoolName() + ", desired: " + ((Shared)clientConfiguration.getPoolAllocation()).getResourcePoolName()));
    }
  }

  @Test
  public void testAllResourceParametersMatch()
  {
    ServerStoreConfiguration serverConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreConfiguration clientConfiguration = new ServerStoreConfiguration(FIXED_POOL_ALLOCATION,
                                                                                STORED_KEY_TYPE,
                                                                                STORED_VALUE_TYPE,
                                                                                ACTUAL_KEY_TYPE,
                                                                                ACTUAL_VALUE_TYPE,
                                                                                KEY_SERIALIZER_TYPE,
                                                                                VALUE_SERIALIZER_TYPE,
                                                                                Consistency.EVENTUAL);

    ServerStoreCompatibility serverStoreCompatibility = new ServerStoreCompatibility();

    try
    {
      serverStoreCompatibility.verify(serverConfiguration, clientConfiguration);
    }
    catch(Exception e)
    {
      fail("No Exception Expected!");
    }
  }

}
