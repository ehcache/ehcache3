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
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
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
    assertThat(resolvedChain.isCompacted(), is(true));

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

    assertThat(resolvedChain.isCompacted(), is(false));
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
    assertThat(resolvedChain.isCompacted(), is(false));
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
    assertThat(resolvedChain.isCompacted(), is(false));
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
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(3));
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
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(1));
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
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(2));
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
    assertThat(resolvedChain.isCompacted(), is(true));
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
    assertThat(resolvedChain.isCompacted(), is(false));
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
    assertThat(resolvedChain.isCompacted(), is(true));
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
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolveForSingleOperationDoesNotCompact() {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    assertThat(resolvedChain.isCompacted(), is(false));
    assertThat(resolvedChain.getCompactionCount(), is(0));
  }

  @Test
  public void testResolveForMultiplesOperationsAlwaysCompact() {
    //create a random mix of operations
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutIfAbsentOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Suresh", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Mathew", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Melbin", timeSource.getTimeMillis()));
    list.add(new ReplaceOperation<Long, String>(1L, "Joseph", timeSource.getTimeMillis()));
    list.add(new RemoveOperation<Long, String>(2L, timeSource.getTimeMillis()));
    list.add(new ConditionalRemoveOperation<Long, String>(1L, "Albin", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(1L, "Gregory", timeSource.getTimeMillis()));
    list.add(new ConditionalReplaceOperation<Long, String>(1L, "Albin", "Abraham", timeSource.getTimeMillis()));
    list.add(new RemoveOperation<Long, String>(1L, timeSource.getTimeMillis()));
    list.add(new PutIfAbsentOperation<Long, String>(2L, "Albin", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, timeSource.getTimeMillis());
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(8));
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

    assertThat(operation.isExpiryAvailable(), is(true));
    assertThat(operation.expirationTime(), is(Long.MAX_VALUE));
    try {
      operation.timeStamp();
      fail();
    } catch (Exception ex) {
      assertThat(ex.getMessage(), is("Timestamp not available"));
    }
    assertThat(resolvedChain.isCompacted(), is(true));
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

    assertThat(operation.isExpiryAvailable(), is(true));
    assertThat(operation.expirationTime(), is(4L));

    try {
      operation.timeStamp();
      fail();
    } catch (Exception ex) {
      assertThat(ex.getMessage(), is("Timestamp not available"));
    }
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolveDoesNotDecodeOtherKeyOperationValues() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(2L, "Albin", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Suresh", timeSource.getTimeMillis()));
    list.add(new PutOperation<Long, String>(2L, "Mathew", timeSource.getTimeMillis()));
    Chain chain = getChainFromOperations(list);

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<Long, String>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(customCodec, Expirations.noExpiration());
    resolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(0));
    assertThat(valueSerializer.encodeCount, is(0));
  }

  @Test
  public void testResolveDecodesOperationValueOnlyOnDemand() throws Exception {
    ArrayList<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Albin", 1));
    list.add(new PutOperation<Long, String>(1L, "Suresh", 2));
    list.add(new PutOperation<Long, String>(1L, "Mathew", 3));
    Chain chain = getChainFromOperations(list);

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<Long, String>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(customCodec, Expirations.noExpiration());
    resolver.resolve(chain, 1L, timeSource.getTimeMillis());

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(1)); //Only one decode on creation of the resolved operation
    assertThat(valueSerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
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

  private static class CountingLongSerializer extends LongSerializer {

    private int encodeCount = 0;
    private int decodeCount = 0;

    @Override
    public ByteBuffer serialize(final Long object) {
      encodeCount++;
      return super.serialize(object);
    }

    @Override
    public Long read(final ByteBuffer binary) throws ClassNotFoundException {
      decodeCount++;
      return super.read(binary);
    }

    @Override
    public boolean equals(final Long object, final ByteBuffer binary) throws ClassNotFoundException {
      return super.equals(object, binary);
    }
  }

  private static class CountingStringSerializer extends StringSerializer {

    private int encodeCount = 0;
    private int decodeCount = 0;

    @Override
    public ByteBuffer serialize(final String object) {
      encodeCount++;
      return super.serialize(object);
    }

    @Override
    public String read(final ByteBuffer binary) throws ClassNotFoundException {
      decodeCount++;
      return super.read(binary);
    }

    @Override
    public boolean equals(final String object, final ByteBuffer binary) throws ClassNotFoundException {
      return super.equals(object, binary);
    }
  }
}
