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

import org.ehcache.clustered.client.TestTimeSource;
import org.ehcache.clustered.client.internal.store.ChainBuilder;
import org.ehcache.clustered.client.internal.store.ResolvedChain;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ChainResolverTest {

  private static OperationsCodec<Long, String> codec = null;

  private static TestTimeSource timeSource = null;

  @BeforeClass
  public static void initialSetup() {
    codec = new OperationsCodec<Long, String>(new LongSerializer(), new StringSerializer());
    timeSource = new TestTimeSource();
  }

  @Test
  public void testResolveMaintainsOtherKeysInOrder() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Albin", timeSource.getTimeMillis()));
    Operation<Long, String> expected = new PutOperation<Long, String>(1L, "Suresh", timeSource.getTimeMillis());
    list.add(expected);
    list.add(new PutOperation<Long, String>(2L, "Suresh", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Mathew", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);

    Chain compactedChain = resolvedChain.getCompactedChain();
    List<Operation<Long, String>> operations = getOperationsListFromChain(compactedChain);

    List<Operation<Long, String>> expectedOps = new ArrayList<Operation<Long, String>>();
    expectedOps.add(new PutOperation<Long, String>(2L, "Albin", timeSource.getTimeMillis()));
    expectedOps.add(new PutOperation<Long, String>(2L, "Suresh", timeSource.getTimeMillis()));
    expectedOps.add(new PutOperation<Long, String>(2L, "Mathew", timeSource.getTimeMillis()));
    expectedOps.add(new PutOperation<Long, String>(1L, "Suresh", timeSource.getTimeMillis()));

    assertThat(operations, IsIterableContainingInOrder.contains(expectedOps.toArray()));
  }

  @Test
  public void testResolveEmptyChain() throws Exception {
    Chain chain = (new ChainBuilder()).build();
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);

    Chain compactedChain = resolvedChain.getCompactedChain();
    assertTrue(compactedChain.isEmpty());
  }

  @Test
  public void testResolveChainWithNonExistentKey() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Suresh", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Mathew", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 3L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(3L);
    assertNull(result);

    Chain compactedChain = resolvedChain.getCompactedChain();
    List<Operation<Long, String>> expectedOperations = getOperationsListFromChain(compactedChain);
    assertThat(expectedOperations, IsIterableContainingInOrder.contains(list.toArray()));
  }

  @Test
  public void testResolveSinglePut() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    Operation<Long, String> expected = new PutOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis());
    list.add(expected);
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolvePutsOnly() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Suresh", timeSource.getTimeMillis()));
    Operation<Long, String> expected = new PutOperation<Long, String>(1L, "Mathew", timeSource.getTimeMillis());
    list.add(expected);

    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolveSingleRemove() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new RemoveOperation<Long, String>(1L, timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
  }

  @Test
  public void testResolveRemovesOnly() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new RemoveOperation<Long, String>(1L, timeSource.getTimeMillis()));
    list.add(new RemoveOperation<Long, String>(1L, timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
  }

  @Test
  public void testPutAndRemove() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis()));
    list.add(new RemoveOperation<Long, String>(1L, timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
  }

  @Test
  public void testResolvePutIfAbsentOnly() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    Operation<Long, String> expected = new PutIfAbsentOperation<Long, String>(1L, "Mathew", timeSource.getTimeMillis());
    list.add(expected);
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolvePutIfAbsentsOnly() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    Operation<Long, String> expected = new PutIfAbsentOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis());
    list.add(expected);
    list.add(new PutIfAbsentOperation<Long, String>(1L, "Suresh", timeSource.getTimeMillis()));
    list.add(new PutIfAbsentOperation<Long, String>(1L, "Mathew", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolvePutIfAbsentSucceeds() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis()));
    list.add(new RemoveOperation<Long, String>(1L, timeSource.getTimeMillis()));
    Operation<Long, String> expected = new PutIfAbsentOperation<Long, String>(1L, "Mathew", timeSource.getTimeMillis());
    list.add(expected);
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
  }

  @Test
  public void testResolveForSingleOperationHasCorrectIsFirstAndTimeStamp() {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());

    Operation<Long, String> operation = getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0);

    assertThat(operation.isExpiryAvailable(), Matchers.is(true));
    assertThat(operation.expirationTime(), Matchers.is(Long.MIN_VALUE));
  }

  @Test
  public void testResolveForMultipleOperationHasCorrectIsFirstAndTimeStamp() {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    TestTimeSource testTimeSource = new TestTimeSource();

    list.add(new PutOperation<Long, String>(1L, "Albin1", testTimeSource.getTimeMillis()));
    testTimeSource.advanceTime(1L);
    list.add(new PutOperation<Long, String>(1L, "Albin2", testTimeSource.getTimeMillis()));
    testTimeSource.advanceTime(1L);
    list.add(new RemoveOperation<Long, String>(1L, testTimeSource.getTimeMillis()));
    testTimeSource.advanceTime(1L);
    list.add(new PutOperation<Long, String>(1L, "AlbinAfterRemove", testTimeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, testTimeSource.getTimeMillis());

    Operation<Long, String> operation = getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0);

    assertThat(operation.isExpiryAvailable(), Matchers.is(true));
    assertThat(operation.expirationTime(), Matchers.is(Long.MIN_VALUE));
    try {
      operation.timeStamp();
      fail();
    } catch (Exception ex) {
      assertThat(ex.getMessage(), Matchers.is("Timestamp not available"));
    }
  }

  @Test
  public void testResolveForMultipleOperationHasCorrectIsFirstAndTimeStampWithExpiry() {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    TestTimeSource testTimeSource = new TestTimeSource();

    list.add(new PutOperation<Long, String>(1L, "Albin1", testTimeSource.getTimeMillis()));
    testTimeSource.advanceTime(1L);
    list.add(new PutOperation<Long, String>(1L, "Albin2", testTimeSource.getTimeMillis()));
    testTimeSource.advanceTime(1L);
    list.add(new PutOperation<Long, String>(1L, "Albin3", testTimeSource.getTimeMillis()));
    testTimeSource.advanceTime(1L);
    list.add(new PutOperation<Long, String>(1L, "Albin4", testTimeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.timeToLiveExpiration(new Duration(1l, TimeUnit.MILLISECONDS)));
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, testTimeSource.getTimeMillis());

    Operation<Long, String> operation = getOperationsListFromChain(resolvedChain.getCompactedChain()).get(0);

    assertThat(operation.isExpiryAvailable(), Matchers.is(true));
    assertThat(operation.expirationTime(), Matchers.is(4L));

    try {
      operation.timeStamp();
      fail();
    } catch (Exception ex) {
      assertThat(ex.getMessage(), Matchers.is("Timestamp not available"));
    }

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
