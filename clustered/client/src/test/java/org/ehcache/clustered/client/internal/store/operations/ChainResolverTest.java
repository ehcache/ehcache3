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
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ChainResolverTest {

  private static OperationsCodec<Long, String> codec = new OperationsCodec<Long, String>(new LongSerializer(), new StringSerializer());

  @Test
  @SuppressWarnings("unchecked")
  public void testResolveMaintainsOtherKeysInOrder() throws Exception {
    Operation<Long, String> expected = new PutOperation<Long, String>(1L, "Suresh", 0L);
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L),
      new PutOperation<Long, String>(2L, "Albin", 0L),
      expected,
      new PutOperation<Long, String>(2L, "Suresh", 0L),
      new PutOperation<Long, String>(2L, "Mathew", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));

    Chain compactedChain = resolvedChain.getCompactedChain();
    assertThat(compactedChain, contains( //@SuppressWarnings("unchecked")
      operation(new PutOperation<Long, String>(2L, "Albin", 0L)),
      operation(new PutOperation<Long, String>(2L, "Suresh", 0L)),
      operation(new PutOperation<Long, String>(2L, "Mathew", 0L)),
      operation(new PutOperation<Long, String>(1L, "Suresh", 0L))));
  }

  @Test
  public void testResolveEmptyChain() throws Exception {
    Chain chain = getChainFromOperations();
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);

    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolveChainWithNonExistentKey() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L),
      new PutOperation<Long, String>(2L, "Suresh", 0L),
      new PutOperation<Long, String>(2L, "Mathew", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 3L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(3L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolveSinglePut() throws Exception {
    Operation<Long, String> expected = new PutOperation<Long, String>(1L, "Albin", 0L);
    Chain chain = getChainFromOperations(expected);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolvePutsOnly() throws Exception {
    Operation<Long, String> expected = new PutOperation<Long, String>(1L, "Mathew", 0L);

    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L),
      new PutOperation<Long, String>(1L, "Suresh", 0L),
      expected);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(3));
  }

  @Test
  public void testResolveSingleRemove() throws Exception {
    Chain chain = getChainFromOperations(new RemoveOperation<Long, String>(1L, 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(1));
  }

  @Test
  public void testResolveRemovesOnly() throws Exception {
    Chain chain = getChainFromOperations(
      new RemoveOperation<Long, String>(1L, 0L),
      new RemoveOperation<Long, String>(1L, 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(2));
  }

  @Test
  public void testPutAndRemove() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L),
      new RemoveOperation<Long, String>(1L, 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolvePutIfAbsentOnly() throws Exception {
    Operation<Long, String> expected = new PutIfAbsentOperation<Long, String>(1L, "Mathew", 0L);
    Chain chain = getChainFromOperations(expected);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolvePutIfAbsentsOnly() throws Exception {
    Operation<Long, String> expected = new PutIfAbsentOperation<Long, String>(1L, "Albin", 0L);
    Chain chain = getChainFromOperations(
      expected,
      new PutIfAbsentOperation<Long, String>(1L, "Suresh", 0L),
      new PutIfAbsentOperation<Long, String>(1L, "Mathew", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolvePutIfAbsentSucceeds() throws Exception {
    Operation<Long, String> expected = new PutIfAbsentOperation<Long, String>(1L, "Mathew", 0L);
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L),
      new RemoveOperation<Long, String>(1L, 0L),
      expected);

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolveForSingleOperationDoesNotCompact() {
    Chain chain = getChainFromOperations(new PutOperation<Long, String>(1L, "Albin", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    assertThat(resolvedChain.isCompacted(), is(false));
    assertThat(resolvedChain.getCompactionCount(), is(0));
  }

  @Test
  public void testResolveForMultiplesOperationsAlwaysCompact() {
    //create a random mix of operations
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<Long, String>(1L, "Albin", 0L),
      new PutOperation<Long, String>(1L, "Suresh", 0L),
      new PutOperation<Long, String>(1L, "Mathew", 0L),
      new PutOperation<Long, String>(2L, "Melbin", 0L),
      new ReplaceOperation<Long, String>(1L, "Joseph", 0L),
      new RemoveOperation<Long, String>(2L, 0L),
      new ConditionalRemoveOperation<Long, String>(1L, "Albin", 0L),
      new PutOperation<Long, String>(1L, "Gregory", 0L),
      new ConditionalReplaceOperation<Long, String>(1L, "Albin", "Abraham", 0L),
      new RemoveOperation<Long, String>(1L, 0L),
      new PutIfAbsentOperation<Long, String>(2L, "Albin", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(8));
  }

  @Test
  public void testResolveForMultipleOperationHasCorrectIsFirstAndTimeStamp() {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin1", 0),
      new PutOperation<Long, String>(1L, "Albin2", 1),
      new RemoveOperation<Long, String>(1L, 2),
      new PutOperation<Long, String>(1L, "AlbinAfterRemove", 3));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 3);

    Operation<Long, String> operation = codec.decode(resolvedChain.getCompactedChain().iterator().next().getPayload());

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
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin1", 0L),
      new PutOperation<Long, String>(1L, "Albin2", 1L),
      new PutOperation<Long, String>(1L, "Albin3", 2L),
      new PutOperation<Long, String>(1L, "Albin4", 3L)
    );

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.timeToLiveExpiration(new Duration(1l, TimeUnit.MILLISECONDS)));
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 3L);

    Operation<Long, String> operation = codec.decode(resolvedChain.getCompactedChain().iterator().next().getPayload());

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
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(2L, "Albin", 0L),
      new PutOperation<Long, String>(2L, "Suresh", 0L),
      new PutOperation<Long, String>(2L, "Mathew", 0L));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<Long, String>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(customCodec, Expirations.noExpiration());
    resolver.resolve(chain, 1L, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(0));
    assertThat(valueSerializer.encodeCount, is(0));
  }

  @Test
  public void testResolveDecodesOperationValueOnlyOnDemand() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 1),
      new PutOperation<Long, String>(1L, "Suresh", 2),
      new PutOperation<Long, String>(1L, "Mathew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<Long, String>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(customCodec, Expirations.noExpiration());
    resolver.resolve(chain, 1L, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(1)); //Only one decode on creation of the resolved operation
    assertThat(valueSerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompactingTwoKeys() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L),
      new PutOperation<Long, String>(2L, "Albin", 0L),
      new PutOperation<Long, String>(1L, "Suresh", 0L),
      new PutOperation<Long, String>(2L, "Suresh", 0L),
      new PutOperation<Long, String>(2L, "Mathew", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());

    Chain compactedChain = resolver.compact(chain, 0L);

    assertThat(compactedChain, containsInAnyOrder( //@SuppressWarnings("unchecked")
      operation(new PutOperation<Long, String>(2L, "Mathew", 0L)),
      operation(new PutOperation<Long, String>(1L, "Suresh", 0L))
    ));
  }

  @Test
  public void testCompactEmptyChain() throws Exception {
    Chain chain = (new ChainBuilder()).build();
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compacted = resolver.compact(chain, 0L);
    assertThat(compacted, emptyIterable());
  }

  @Test
  public void testCompactSinglePut() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L)
    );

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compacted = resolver.compact(chain, 0L);

    assertThat(compacted, contains(operation(new PutOperation<Long, String>(1L, "Albin", 0L))));
  }

  @Test
  public void testCompactMultiplePuts() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L),
      new PutOperation<Long, String>(1L, "Suresh", 0L),
      new PutOperation<Long, String>(1L, "Mathew", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compactedChain = resolver.compact(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<Long, String>(1L, "Mathew", 0L))));
  }

  @Test
  public void testCompactSingleRemove() throws Exception {
    Chain chain = getChainFromOperations(new RemoveOperation<Long, String>(1L, 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compactedChain = resolver.compact(chain, 0L);
    assertThat(compactedChain, emptyIterable());
  }

  @Test
  public void testCompactMultipleRemoves() throws Exception {
    Chain chain = getChainFromOperations(
      new RemoveOperation<Long, String>(1L, 0L),
      new RemoveOperation<Long, String>(1L, 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compactedChain = resolver.compact(chain, 0L);
    assertThat(compactedChain, emptyIterable());
  }

  @Test
  public void testCompactPutAndRemove() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L),
      new RemoveOperation<Long, String>(1L, 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compactedChain = resolver.compact(chain, 0L);
    assertThat(compactedChain, emptyIterable());
  }

  @Test
  public void testCompactSinglePutIfAbsent() throws Exception {
    Chain chain = getChainFromOperations(new PutIfAbsentOperation<Long, String>(1L, "Mathew", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compactedChain = resolver.compact(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<Long, String>(1L, "Mathew", 0L))));
  }

  @Test
  public void testCompactMultiplePutIfAbsents() throws Exception {
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<Long, String>(1L, "Albin", 0L),
      new PutIfAbsentOperation<Long, String>(1L, "Suresh", 0L),
      new PutIfAbsentOperation<Long, String>(1L, "Mathew", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compactedChain = resolver.compact(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<Long, String>(1L, "Albin", 0L))));
  }

  @Test
  public void testCompactPutIfAbsentAfterRemove() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 0L),
      new RemoveOperation<Long, String>(1L, 0L),
      new PutIfAbsentOperation<Long, String>(1L, "Mathew", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compactedChain = resolver.compact(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<Long, String>(1L, "Mathew", 0L))));
  }

  @Test
  public void testCompactForMultipleKeysAndOperations() {
    //create a random mix of operations
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<Long, String>(1L, "Albin", 0L),
      new PutOperation<Long, String>(1L, "Suresh", 0L),
      new PutOperation<Long, String>(1L, "Mathew", 0L),
      new PutOperation<Long, String>(2L, "Melbin", 0L),
      new ReplaceOperation<Long, String>(1L, "Joseph", 0L),
      new RemoveOperation<Long, String>(2L, 0L),
      new ConditionalRemoveOperation<Long, String>(1L, "Albin", 0L),
      new PutOperation<Long, String>(1L, "Gregory", 0L),
      new ConditionalReplaceOperation<Long, String>(1L, "Albin", "Abraham", 0L),
      new RemoveOperation<Long, String>(1L, 0L),
      new PutIfAbsentOperation<Long, String>(2L, "Albin", 0L));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compactedChain = resolver.compact(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<Long, String>(2L, "Albin", 0L))));
  }

  @Test
  public void testCompactHasCorrectTimeStamp() {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin1", 0),
      new PutOperation<Long, String>(1L, "Albin2", 1),
      new RemoveOperation<Long, String>(1L, 2),
      new PutOperation<Long, String>(1L, "Albin3", 3));

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    Chain compactedChain = resolver.compact(chain, 3);

    assertThat(compactedChain, contains(operation(new PutOperation<Long, String>(1L, "Albin3", 3))));
  }

  @Test
  public void testCompactHasCorrectWithExpiry() {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin1", 0L),
      new PutOperation<Long, String>(1L, "Albin2", 1L),
      new PutOperation<Long, String>(1L, "Albin3", 2L),
      new PutOperation<Long, String>(1L, "Albin4", 3L)
    );

    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.timeToLiveExpiration(new Duration(1l, TimeUnit.MILLISECONDS)));
    Chain compactedChain = resolver.compact(chain, 3L);

    assertThat(compactedChain, contains(operation(new PutOperation<Long, String>(1L, "Albin4", 3L))));
  }

  @Test
  public void testCompactDecodesOperationValueOnlyOnDemand() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<Long, String>(1L, "Albin", 1),
      new PutOperation<Long, String>(1L, "Suresh", 2),
      new PutOperation<Long, String>(1L, "Mathew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<Long, String>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(customCodec, Expirations.noExpiration());
    resolver.compact(chain, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(0)); //Only one decode on creation of the resolved operation
    assertThat(valueSerializer.encodeCount, is(0)); //One encode from encoding the resolved operation's key
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
  }

  @SafeVarargs
  private final Chain getChainFromOperations(Operation<Long, String> ... operations) {
    ChainBuilder chainBuilder = new ChainBuilder();
    for(Operation<Long, String> operation: operations) {
      chainBuilder = chainBuilder.add(codec.encode(operation));
    }
    return chainBuilder.build();
  }

  private Matcher<Element> operation(Operation<?, ?> operation) {
    return new TypeSafeMatcher<Element>() {
      @Override
      protected boolean matchesSafely(Element item) {
        return operation.equals(codec.decode(item.getPayload()));
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("is ").appendValue(operation);
      }
    };
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
