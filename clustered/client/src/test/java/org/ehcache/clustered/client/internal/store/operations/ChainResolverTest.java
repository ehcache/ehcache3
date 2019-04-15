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

package org.ehcache.clustered.client.internal.store.operations;

import org.ehcache.clustered.client.internal.store.ChainBuilder;
import org.ehcache.clustered.client.internal.store.ResolvedChain;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.*;

public class ChainResolverTest {

  private static OperationsCodec<Long, String> codec = null;

  @BeforeClass
  public static void initialSetup() {
    codec = new OperationsCodec<Long, String>(new LongSerializer(), new StringSerializer());
  }

  @Test
  public void testResolveMaintainsOtherKeysInOrder() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", System.currentTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Albin", System.currentTimeMillis()));
    Operation<Long, String> expected = new PutOperation<Long, String>(1L, "Suresh", System.currentTimeMillis());
    list.add(expected);
    list.add(new PutOperation<Long, String>(2L, "Suresh", System.currentTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Mathew", System.currentTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);

    Chain compactedChain = resolvedChain.getCompactedChain();
    List<Operation<Long, String>> operations = getOperationsListFromChain(compactedChain);

    List<Operation<Long, String>> expectedOps = new ArrayList<Operation<Long, String>>();
    expectedOps.add(new PutOperation<Long, String>(2L, "Albin", System.currentTimeMillis()));
    expectedOps.add(new PutOperation<Long, String>(2L, "Suresh", System.currentTimeMillis()));
    expectedOps.add(new PutOperation<Long, String>(2L, "Mathew", System.currentTimeMillis()));
    expectedOps.add(new PutOperation<Long, String>(1L, "Suresh", System.currentTimeMillis()));

    assertThat(operations, IsIterableContainingInOrder.contains(expectedOps.toArray()));
  }

  @Test
  public void testResolveEmptyChain() throws Exception {
    Chain chain = (new ChainBuilder()).build();
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);

    Chain compactedChain = resolvedChain.getCompactedChain();
    assertTrue(compactedChain.isEmpty());
  }

  @Test
  public void testResolveChainWithNonExistentKey() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", System.currentTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Suresh", System.currentTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Mathew", System.currentTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 3L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(3L);
    assertNull(result);

    Chain compactedChain = resolvedChain.getCompactedChain();
    List<Operation<Long, String>> expectedOperations = getOperationsListFromChain(compactedChain);
    assertThat(expectedOperations, IsIterableContainingInOrder.contains(list.toArray()));
  }

  @Test
  public void testResolveSinglePut() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    Operation<Long, String> expected = new PutOperation<Long, String>(1L, "Albin", System.currentTimeMillis());
    list.add(expected);
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolvePutsOnly() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", System.currentTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Suresh", System.currentTimeMillis()));
    Operation<Long, String> expected = new PutOperation<Long, String>(1L, "Mathew", System.currentTimeMillis());
    list.add(expected);

    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolveSingleRemove() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new RemoveOperation<Long, String>(1L, System.currentTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
  }

  @Test
  public void testResolveRemovesOnly() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new RemoveOperation<Long, String>(1L, System.currentTimeMillis()));
    list.add(new RemoveOperation<Long, String>(1L, System.currentTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
  }

  @Test
  public void testPutAndRemove() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", System.currentTimeMillis()));
    list.add(new RemoveOperation<Long, String>(1L, System.currentTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
  }

  @Test
  public void testResolvePutIfAbsentOnly() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    Operation<Long, String> expected = new PutIfAbsentOperation<Long, String>(1L, "Mathew", System.currentTimeMillis());
    list.add(expected);
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolvePutIfAbsentsOnly() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    Operation<Long, String> expected = new PutIfAbsentOperation<Long, String>(1L, "Albin", System.currentTimeMillis());
    list.add(expected);
    list.add(new PutIfAbsentOperation<Long, String>(1L, "Suresh", System.currentTimeMillis()));
    list.add(new PutIfAbsentOperation<Long, String>(1L, "Mathew", System.currentTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolvePutIfAbsentSucceeds() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", System.currentTimeMillis()));
    list.add(new RemoveOperation<Long, String>(1L, System.currentTimeMillis()));
    Operation<Long, String> expected = new PutIfAbsentOperation<Long, String>(1L, "Mathew", System.currentTimeMillis());
    list.add(expected);
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, SystemTimeSource.INSTANCE.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolveExpiresUsingOperationTime() {
    Chain chain = getChainFromOperations(asList(
        new PutOperation<Long, String>(1L, "Albin", 0),
        new PutIfAbsentOperation<Long, String>(1L, "Chris", 900)
    ));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.timeToLiveExpiration(Duration.of(1, TimeUnit.SECONDS)));

    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 1500);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertThat(result.getValue(), nullValue());
  }

  private Chain getChainFromOperations(List<Operation<Long, String>> operations) {
    ChainBuilder chainBuilder = new ChainBuilder();
    for(Operation<Long, String> operation: operations) {
      chainBuilder = chainBuilder.add(codec.encode(operation));
    }
    return chainBuilder.build();
  }

  private List<Operation<Long, String>> getOperationsListFromChain(Chain chain) {
    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    for (Element element : chain) {
      Operation<Long, String> operation = codec.decode(element.getPayload());
      list.add(operation);
    }
    return list;
  }
}
