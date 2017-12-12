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
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExpiryChainResolverTest {

  private static OperationsCodec<Long, String> codec = new OperationsCodec<>(new LongSerializer(), new StringSerializer());

  @Test
  @SuppressWarnings("unchecked")
  public void testResolveMaintainsOtherKeysInOrder() throws Exception {
    Operation<Long, String> expected = new PutOperation<>(1L, "Suresh", 0L);
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Albin", 0L),
      expected,
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));

    Chain compactedChain = resolvedChain.getCompactedChain();
    assertThat(compactedChain, contains( //@SuppressWarnings("unchecked")
      operation(new PutOperation<>(2L, "Albin", 0L)),
      operation(new PutOperation<>(2L, "Suresh", 0L)),
      operation(new PutOperation<>(2L, "Mathew", 0L)),
      operation(new PutOperation<>(1L, "Suresh", 0L))));
  }

  @Test
  public void testResolveEmptyChain() throws Exception {
    Chain chain = getChainFromOperations();
    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);

    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolveChainWithNonExistentKey() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 3L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(3L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolveSinglePut() throws Exception {
    Operation<Long, String> expected = new PutOperation<>(1L, "Albin", 0L);
    Chain chain = getChainFromOperations(expected);

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolvePutsOnly() throws Exception {
    Operation<Long, String> expected = new PutOperation<>(1L, "Mathew", 0L);

    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(1L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(2));
  }

  @Test
  public void testResolveSingleRemove() throws Exception {
    Chain chain = getChainFromOperations(new RemoveOperation<>(1L, 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(1));
  }

  @Test
  public void testResolveRemovesOnly() throws Exception {
    Chain chain = getChainFromOperations(
      new RemoveOperation<>(1L, 0L),
      new RemoveOperation<>(1L, 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(2));
  }

  @Test
  public void testPutAndRemove() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolvePutIfAbsentOnly() throws Exception {
    Operation<Long, String> expected = new PutOperation<>(1L, "Mathew", 0L);
    Chain chain = getChainFromOperations(new PutIfAbsentOperation<>(1L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolvePutIfAbsentsOnly() throws Exception {
    Operation<Long, String> expected = new PutOperation<>(1L, "Albin", 0L);
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutIfAbsentOperation<>(1L, "Suresh", 0L),
      new PutIfAbsentOperation<>(1L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolvePutIfAbsentSucceeds() throws Exception {
    Operation<Long, String> expected = new PutOperation<>(1L, "Mathew", 0L);
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(1L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolveForSingleOperationDoesNotCompact() {
    Chain chain = getChainFromOperations(new PutOperation<>(1L, "Albin", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    assertThat(resolvedChain.isCompacted(), is(false));
    assertThat(resolvedChain.getCompactionCount(), is(0));
  }

  @Test
  public void testResolveForMultiplesOperationsAlwaysCompact() {
    //create a random mix of operations
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(1L, "Mathew", 0L),
      new PutOperation<>(2L, "Melbin", 0L),
      new ReplaceOperation<>(1L, "Joseph", 0L),
      new RemoveOperation<>(2L, 0L),
      new ConditionalRemoveOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Gregory", 0L),
      new ConditionalReplaceOperation<>(1L, "Albin", "Abraham", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(2L, "Albin", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(8));
  }

  @Test
  public void testResolveForMultipleOperationHasCorrectIsFirstAndTimeStamp() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin1", 0),
      new PutOperation<>(1L, "Albin2", 1),
      new RemoveOperation<>(1L, 2),
      new PutOperation<>(1L, "AlbinAfterRemove", 3));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofHours(1)));
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 3);

    Operation<Long, String> operation = codec.decode(resolvedChain.getCompactedChain().iterator().next().getPayload());

    assertThat(operation.isExpiryAvailable(), is(true));
    assertThat(operation.expirationTime(), is(TimeUnit.HOURS.toMillis(1) + 3));
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
      new PutOperation<>(1L, "Albin1", 0L),
      new PutOperation<>(1L, "Albin2", 1L),
      new PutOperation<>(1L, "Albin3", 2L),
      new PutOperation<>(1L, "Albin4", 3L)
    );

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1L)));
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
      new PutOperation<>(2L, "Albin", 0L),
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Mathew", 0L));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(customCodec, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofSeconds(5)));
    resolver.resolve(chain, 1L, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(0));
    assertThat(valueSerializer.encodeCount, is(0));
  }

  @Test
  public void testResolveDecodesOperationValueOnlyOnDemand() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 1),
      new PutOperation<>(1L, "Suresh", 2),
      new PutOperation<>(1L, "Mathew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(customCodec, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofSeconds(5)));
    resolver.resolve(chain, 1L, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(3));
    assertThat(valueSerializer.encodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompactingTwoKeys() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());

    Chain compactedChain = resolver.applyOperation(chain, 0L);

    assertThat(compactedChain, containsInAnyOrder( //@SuppressWarnings("unchecked")
      operation(new PutOperation<>(2L, "Mathew", 0L)),
      operation(new PutOperation<>(1L, "Suresh", 0L))
    ));
  }

  @Test
  public void testCompactEmptyChain() throws Exception {
    Chain chain = (new ChainBuilder()).build();
    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compacted = resolver.applyOperation(chain, 0L);
    assertThat(compacted, emptyIterable());
  }

  @Test
  public void testCompactSinglePut() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L)
    );

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compacted = resolver.applyOperation(chain, 0L);

    assertThat(compacted, contains(operation(new PutOperation<>(1L, "Albin", 0L))));
  }

  @Test
  public void testCompactMultiplePuts() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(1L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Mathew", 0L))));
  }

  @Test
  public void testCompactSingleRemove() throws Exception {
    Chain chain = getChainFromOperations(new RemoveOperation<>(1L, 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, emptyIterable());
  }

  @Test
  public void testCompactMultipleRemoves() throws Exception {
    Chain chain = getChainFromOperations(
      new RemoveOperation<>(1L, 0L),
      new RemoveOperation<>(1L, 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, emptyIterable());
  }

  @Test
  public void testCompactPutAndRemove() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, emptyIterable());
  }

  @Test
  public void testCompactSinglePutIfAbsent() throws Exception {
    Chain chain = getChainFromOperations(new PutIfAbsentOperation<>(1L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Mathew", 0L))));
  }

  @Test
  public void testCompactMultiplePutIfAbsents() throws Exception {
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutIfAbsentOperation<>(1L, "Suresh", 0L),
      new PutIfAbsentOperation<>(1L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Albin", 0L))));
  }

  @Test
  public void testCompactPutIfAbsentAfterRemove() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(1L, "Mathew", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Mathew", 0L))));
  }

  @Test
  public void testCompactForMultipleKeysAndOperations() {
    //create a random mix of operations
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(1L, "Mathew", 0L),
      new PutOperation<>(2L, "Melbin", 0L),
      new ReplaceOperation<>(1L, "Joseph", 0L),
      new RemoveOperation<>(2L, 0L),
      new ConditionalRemoveOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Gregory", 0L),
      new ConditionalReplaceOperation<>(1L, "Albin", "Abraham", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(2L, "Albin", 0L));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<>(2L, "Albin", 0L))));
  }

  @Test
  public void testCompactHasCorrectTimeStamp() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin1", 0),
      new PutOperation<>(1L, "Albin2", 1),
      new RemoveOperation<>(1L, 2),
      new PutOperation<>(1L, "Albin3", 3));

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 3);

    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Albin3", 3))));
  }

  @Test
  public void testCompactHasCorrectWithExpiry() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin1", 0L),
      new PutOperation<>(1L, "Albin2", 1L),
      new PutOperation<>(1L, "Albin3", 2L),
      new PutOperation<>(1L, "Albin4", 3L)
    );

    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1L)));
    Chain compactedChain = resolver.applyOperation(chain, 3L);

    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Albin4", 3L))));
  }

  @Test
  public void testCompactDecodesOperationValueOnlyOnDemand() throws Exception {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 1),
      new PutOperation<>(1L, "Suresh", 2),
      new PutOperation<>(1L, "Mathew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ExpiryChainResolver<Long, String> resolver = new ExpiryChainResolver<>(customCodec, ExpiryPolicyBuilder.noExpiration());
    resolver.applyOperation(chain, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(3));
    assertThat(valueSerializer.encodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(1));
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
