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

package org.ehcache.clustered.common.internal.store.operations;

import org.ehcache.clustered.common.internal.util.ChainBuilder;
import org.ehcache.clustered.client.internal.store.ResolvedChain;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public abstract class AbstractChainResolverTest {

  private static OperationsCodec<Long, String> codec = new OperationsCodec<>(new LongSerializer(), new StringSerializer());

  protected abstract ChainResolver<Long, String> createChainResolver(ExpiryPolicy<? super Long, ? super String> expiryPolicy, OperationsCodec<Long, String> codec);

  @Test
  @SuppressWarnings("unchecked")
  public void testResolveMaintainsOtherKeysInOrder() {
    Operation<Long, String> expected = new PutOperation<>(1L, "Suresh", 0L);
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Albin", 0L),
      expected,
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));

    Chain compactedChain = resolvedChain.getCompactedChain();
    assertThat(compactedChain, contains( //@SuppressWarnings("unchecked")
      operation(new PutOperation<>(2L, "Albin", 0L)),
      operation(new PutOperation<>(2L, "Suresh", 0L)),
      operation(new PutOperation<>(2L, "Matthew", 0L)),
      operation(new PutOperation<>(1L, "Suresh", 0L))));
  }

  @Test
  public void testResolveEmptyChain() {
    Chain chain = getChainFromOperations();
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);

    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolveChainWithNonExistentKey() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 3L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(3L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolveSinglePut() {
    Operation<Long, String> expected = new PutOperation<>(1L, "Albin", 0L);
    Chain chain = getChainFromOperations(expected);

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolvePutsOnly() {
    Operation<Long, String> expected = new PutOperation<>(1L, "Matthew", 0L);

    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(2));
  }

  @Test
  public void testResolveSingleRemove() {
    Chain chain = getChainFromOperations(new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(1));
  }

  @Test
  public void testResolveRemovesOnly() {
    Chain chain = getChainFromOperations(
      new RemoveOperation<>(1L, 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(2));
  }

  @Test
  public void testPutAndRemove() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertNull(result);
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolvePutIfAbsentOnly() {
    Operation<Long, String> expected = new PutOperation<>(1L, "Matthew", 0L);
    Chain chain = getChainFromOperations(new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(false));
  }

  @Test
  public void testResolvePutIfAbsentsOnly() {
    Operation<Long, String> expected = new PutOperation<>(1L, "Albin", 0L);
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutIfAbsentOperation<>(1L, "Suresh", 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolvePutIfAbsentSucceeds() {
    Operation<Long, String> expected = new PutOperation<>(1L, "Matthew", 0L);
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    Result<Long, String> result = resolvedChain.getResolvedResult(1L);
    assertEquals(expected, result);
    assertThat(resolvedChain.isCompacted(), is(true));
  }

  @Test
  public void testResolveForSingleOperationDoesNotCompact() {
    Chain chain = getChainFromOperations(new PutOperation<>(1L, "Albin", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
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
      new PutOperation<>(1L, "Matthew", 0L),
      new PutOperation<>(2L, "Melvin", 0L),
      new ReplaceOperation<>(1L, "Joseph", 0L),
      new RemoveOperation<>(2L, 0L),
      new ConditionalRemoveOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Gregory", 0L),
      new ConditionalReplaceOperation<>(1L, "Albin", "Abraham", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(2L, "Albin", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    ResolvedChain<Long, String> resolvedChain = resolver.resolve(chain, 1L, 0L);
    assertThat(resolvedChain.isCompacted(), is(true));
    assertThat(resolvedChain.getCompactionCount(), is(8));
  }

  @Test
  public void testResolveDoesNotDecodeOtherKeyOperationValues() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(2L, "Albin", 0L),
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Matthew", 0L));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration(), customCodec);
    resolver.resolve(chain, 1L, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(0));
    assertThat(valueSerializer.encodeCount, is(0)); //No operation to resolve
  }

  @Test
  public void testResolveDecodesOperationValueOnlyOnDemand() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 1),
      new PutOperation<>(1L, "Suresh", 2),
      new PutOperation<>(1L, "Matthew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration(), customCodec);
    resolver.resolve(chain, 1L, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(0));
    assertThat(valueSerializer.encodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompactingTwoKeys() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());

    Chain compactedChain = resolver.applyOperation(chain, 0L);

    assertThat(compactedChain, containsInAnyOrder( //@SuppressWarnings("unchecked")
      operation(new PutOperation<>(2L, "Matthew", 0L)),
      operation(new PutOperation<>(1L, "Suresh", 0L))
    ));
  }

  @Test
  public void testCompactEmptyChain() {
    Chain chain = (new ChainBuilder()).build();
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compacted = resolver.applyOperation(chain, 0L);
    assertThat(compacted, emptyIterable());
  }

  @Test
  public void testCompactSinglePut() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L)
    );

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compacted = resolver.applyOperation(chain, 0L);

    assertThat(compacted, contains(operation(new PutOperation<>(1L, "Albin", 0L))));
  }

  @Test
  public void testCompactMultiplePuts() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Matthew", 0L))));
  }

  @Test
  public void testCompactSingleRemove() {
    Chain chain = getChainFromOperations(new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, emptyIterable());
  }

  @Test
  public void testCompactMultipleRemoves() {
    Chain chain = getChainFromOperations(
      new RemoveOperation<>(1L, 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, emptyIterable());
  }

  @Test
  public void testCompactPutAndRemove() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, emptyIterable());
  }

  @Test
  public void testCompactSinglePutIfAbsent() {
    Chain chain = getChainFromOperations(new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Matthew", 0L))));
  }

  @Test
  public void testCompactMultiplePutIfAbsents() {
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutIfAbsentOperation<>(1L, "Suresh", 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Albin", 0L))));
  }

  @Test
  public void testCompactPutIfAbsentAfterRemove() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 0L);
    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Matthew", 0L))));
  }

  @Test
  public void testCompactForMultipleKeysAndOperations() {
    //create a random mix of operations
    Chain chain = getChainFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(1L, "Matthew", 0L),
      new PutOperation<>(2L, "Melvin", 0L),
      new ReplaceOperation<>(1L, "Joseph", 0L),
      new RemoveOperation<>(2L, 0L),
      new ConditionalRemoveOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Gregory", 0L),
      new ConditionalReplaceOperation<>(1L, "Albin", "Abraham", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(2L, "Albin", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
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

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Chain compactedChain = resolver.applyOperation(chain, 3);

    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Albin3", 3))));
  }

  @Test
  public void testCompactDecodesOperationValueOnlyOnDemand() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin", 1),
      new PutOperation<>(1L, "Suresh", 2),
      new PutOperation<>(1L, "Matthew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration(), customCodec);
    resolver.applyOperation(chain, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(0)); //Only one decode on creation of the resolved operation
    assertThat(valueSerializer.encodeCount, is(0)); //One encode from encoding the resolved operation's key
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
  }

  @Test
  public void testResolveForMultipleOperationHasCorrectIsFirstAndTimeStamp() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin1", 0),
      new PutOperation<>(1L, "Albin2", 1),
      new RemoveOperation<>(1L, 2),
      new PutOperation<>(1L, "AlbinAfterRemove", 3));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofHours(1)));
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

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1L)));
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
  public void testCompactHasCorrectWithExpiry() {
    Chain chain = getChainFromOperations(
      new PutOperation<>(1L, "Albin1", 0L),
      new PutOperation<>(1L, "Albin2", 1L),
      new PutOperation<>(1L, "Albin3", 2L),
      new PutOperation<>(1L, "Albin4", 3L)
    );

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1L)));
    Chain compactedChain = resolver.applyOperation(chain, 3L);

    assertThat(compactedChain, contains(operation(new PutOperation<>(1L, "Albin4", 3L))));
  }

  protected ChainResolver<Long, String> createChainResolver(ExpiryPolicy<? super Long, ? super String> expiryPolicy) {
    return createChainResolver(expiryPolicy, codec);
  }

  @SafeVarargs
  protected final Chain getChainFromOperations(Operation<Long, String> ... operations) {
    ChainBuilder chainBuilder = new ChainBuilder();
    for(Operation<Long, String> operation: operations) {
      chainBuilder = chainBuilder.add(codec.encode(operation));
    }
    return chainBuilder.build();
  }

  protected List<Operation<Long, String>> getOperationsListFromChain(Chain chain) {
    List<Operation<Long, String>> list = new ArrayList<>();
    for (Element element : chain) {
      Operation<Long, String> operation = codec.decode(element.getPayload());
      list.add(operation);
    }
    return list;
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

  protected static class CountingLongSerializer extends LongSerializer {

    protected int encodeCount = 0;
    protected int decodeCount = 0;

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

  protected static class CountingStringSerializer extends StringSerializer {

    protected int encodeCount = 0;
    protected int decodeCount = 0;

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
