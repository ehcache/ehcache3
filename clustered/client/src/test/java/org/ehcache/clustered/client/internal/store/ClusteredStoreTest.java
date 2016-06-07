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

package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.TestTimeSource;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.messages.ServerStoreMessageFactory;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.connection.Connection;

import java.net.URI;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Properties;

import static org.ehcache.clustered.util.StatisticsTestUtils.validateStat;
import static org.ehcache.clustered.util.StatisticsTestUtils.validateStats;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

public class ClusteredStoreTest {

  private static final String CACHE_IDENTIFIER = "testCache";
  private static final URI CLUSTER_URI = URI.create("terracotta://localhost:9510");

  ClusteredStore<Long, String> store;

  @Before
  public void setup() throws Exception {
    UnitTestConnectionService.add(
        CLUSTER_URI,
        new UnitTestConnectionService.PassthroughServerBuilder().resource("defaultResource", 8, MemoryUnit.MB).build()
    );

    Connection connection = new UnitTestConnectionService().connect(CLUSTER_URI, new Properties());
    EhcacheClientEntityFactory entityFactory = new EhcacheClientEntityFactory(connection);

    ServerSideConfiguration serverConfig =
        new ServerSideConfiguration("defaultResource", Collections.<String, ServerSideConfiguration.Pool>emptyMap());
    entityFactory.create("TestCacheManager", serverConfig);

    EhcacheClientEntity clientEntity = entityFactory.retrieve("TestCacheManager", serverConfig);
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.fixed(4, MemoryUnit.MB);
    ServerStoreConfiguration serverStoreConfiguration =
        new ServerStoreConfiguration(resourcePool.getPoolAllocation(),
            Long.class.getName(), String.class.getName(),
            Long.class.getName(), String.class.getName(),
            LongSerializer.class.getName(), StringSerializer.class.getName(),
            null
    );
    clientEntity.createCache(CACHE_IDENTIFIER, serverStoreConfiguration);
    ServerStoreMessageFactory factory = new ServerStoreMessageFactory(CACHE_IDENTIFIER);
    ServerStoreProxy serverStoreProxy = new NoInvalidationServerStoreProxy(factory, clientEntity);

    TestTimeSource testTimeSource = new TestTimeSource();

    OperationsCodec<Long, String> codec = new OperationsCodec<Long, String>(new LongSerializer(), new StringSerializer());
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration(), testTimeSource);
    store = new ClusteredStore<Long, String>(codec, resolver, serverStoreProxy, testTimeSource);
  }

  @After
  public void tearDown() throws Exception {
    UnitTestConnectionService.remove("terracotta://localhost:9510/my-application?auto-create");
  }

  @Test
  public void testPut() throws Exception {
    assertThat(store.put(1L, "one"), is(Store.PutStatus.PUT));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT));
    assertThat(store.put(1L, "another one"), is(Store.PutStatus.UPDATE));
    assertThat(store.put(1L, "yet another one"), is(Store.PutStatus.UPDATE));
    validateStat(store, StoreOperationOutcomes.PutOutcome.REPLACED, 2);
    validateStat(store, StoreOperationOutcomes.PutOutcome.PUT, 1);
  }

  @Test
  public void testGet() throws Exception {
    assertThat(store.get(1L), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.get(1L).value(), is("one"));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.MISS, StoreOperationOutcomes.GetOutcome.HIT));
  }

  @Test
  public void testContainsKey() throws Exception {
    assertThat(store.containsKey(1L), is(false));
    store.put(1L, "one");
    assertThat(store.containsKey(1L), is(true));
    validateStat(store, StoreOperationOutcomes.GetOutcome.HIT, 0);
    validateStat(store, StoreOperationOutcomes.GetOutcome.MISS, 0);
  }

  @Test
  public void testRemove() throws Exception {
    assertThat(store.remove(1L), is(false));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.RemoveOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.remove(1L), is(true));
    assertThat(store.containsKey(1L), is(false));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.RemoveOutcome.MISS, StoreOperationOutcomes.RemoveOutcome.REMOVED));
  }

  @Test
  public void testClear() throws Exception {
    assertThat(store.containsKey(1L), is(false));
    store.clear();
    assertThat(store.containsKey(1L), is(false));

    store.put(1L, "one");
    store.put(2L, "two");
    store.put(3L, "three");
    assertThat(store.containsKey(1L), is(true));

    store.clear();

    assertThat(store.containsKey(1L), is(false));
    assertThat(store.containsKey(2L), is(false));
    assertThat(store.containsKey(3L), is(false));
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    assertThat(store.putIfAbsent(1L, "one"), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT));
    assertThat(store.putIfAbsent(1L, "another one").value(), is("one"));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT, StoreOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  @Test
  public void testConditionalRemove() throws Exception {
    assertThat(store.remove(1L, "one"), is(Store.RemoveStatus.KEY_MISSING));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.remove(1L, "one"), is(Store.RemoveStatus.REMOVED));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS, StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED));
    store.put(1L, "another one");
    assertThat(store.remove(1L, "one"), is(Store.RemoveStatus.KEY_PRESENT));
    validateStat(store, StoreOperationOutcomes.ConditionalRemoveOutcome.MISS, 2);
  }

  @Test
  public void testReplace() throws Exception {
    assertThat(store.replace(1L, "one"), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.replace(1L, "another one").value(), is("one"));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.MISS, StoreOperationOutcomes.ReplaceOutcome.REPLACED));
  }

  @Test
  public void testConditionalReplace() throws Exception {
    assertThat(store.replace(1L, "one" , "another one"), is(Store.ReplaceStatus.MISS_NOT_PRESENT));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS));
    store.put(1L, "some other one");
    assertThat(store.replace(1L, "one" , "another one"), is(Store.ReplaceStatus.MISS_PRESENT));
    validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.MISS, 2);
    validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED, 0);
    assertThat(store.replace(1L, "some other one" , "another one"), is(Store.ReplaceStatus.HIT));
    validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED, 1);
    validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.MISS, 2);
  }
}