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

import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.common.internal.util.ChainBuilder;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public abstract class AbstractChainResolverTest {

  private static OperationsCodec<Long, String> codec = new OperationsCodec<>(new LongSerializer(), new StringSerializer());

  protected abstract ChainResolver<Long, String> createChainResolver(ExpiryPolicy<? super Long, ? super String> expiryPolicy, OperationsCodec<Long, String> codec);

  @Test
  @SuppressWarnings("unchecked")
  public void testResolveMaintainsOtherKeysInOrder() {
    PutOperation<Long, String> expected = new PutOperation<>(1L, "Suresh", 0L);
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Albin", 0L),
      expected,
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder.get(), is(expected.getValue()));
    verify(chain).replaceAtHead(argThat(contains(
      operation(new PutOperation<>(2L, "Albin", 0L)),
      operation(new PutOperation<>(2L, "Suresh", 0L)),
      operation(new PutOperation<>(2L, "Matthew", 0L)),
      operation(new PutOperation<>(1L, "Suresh", 0L)))));
  }

  @Test
  public void testResolveEmptyChain() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations();
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder, nullValue());
    verify(chain, never()).replaceAtHead(any());
  }

  @Test
  public void testResolveChainWithNonExistentKey() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 3L, 0L);
    assertThat(valueHolder, nullValue());
    verify(chain, never()).replaceAtHead(any());
  }

  @Test
  public void testResolveSinglePut() {
    PutOperation<Long, String> expected = new PutOperation<>(1L, "Albin", 0L);
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(expected);

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder.get(), is(expected.getValue()));
    verify(chain, never()).replaceAtHead(any());
  }

  @Test
  public void testResolvePutsOnly() {
    PutOperation<Long, String> expected = new PutOperation<>(1L, "Matthew", 0L);

    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      expected);

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder.get(), is(expected.getValue()));
    verify(chain).replaceAtHead(argThat(contains(operation(expected))));
  }

  @Test
  public void testResolveSingleRemove() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder, nullValue());
    verify(chain).replaceAtHead(argThat(emptyIterable()));
  }

  @Test
  public void testResolveRemovesOnly() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new RemoveOperation<>(1L, 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder, nullValue());
    verify(chain).replaceAtHead(argThat(emptyIterable()));
  }

  @Test
  public void testPutAndRemove() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder, nullValue());
    verify(chain).replaceAtHead(argThat(emptyIterable()));
  }

  @Test
  public void testResolvePutIfAbsentOnly() {
    PutOperation<Long, String> expected = new PutOperation<>(1L, "Matthew", 0L);
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder.get(), is(expected.getValue()));
    verify(chain, never()).replaceAtHead(any());
  }

  @Test
  public void testResolvePutIfAbsentsOnly() {
    PutOperation<Long, String> expected = new PutOperation<>(1L, "Albin", 0L);
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutIfAbsentOperation<>(1L, "Suresh", 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder.get(), is(expected.getValue()));
    verify(chain).replaceAtHead(argThat(contains(operation(expected))));
  }

  @Test
  public void testResolvePutIfAbsentSucceeds() {
    PutOperation<Long, String> expected = new PutOperation<>(1L, "Matthew", 0L);
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder.get(), is(expected.getValue()));
    verify(chain).replaceAtHead(argThat(contains(operation(expected))));
  }

  @Test
  public void testResolveForSingleOperationDoesNotCompact() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(new PutOperation<>(1L, "Albin", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder.get(), is("Albin"));
    verify(chain, never()).replaceAtHead(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testResolveForMultiplesOperationsAlwaysCompact() {
    //create a random mix of operations
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
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
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 0L);
    assertThat(valueHolder, nullValue());
    verify(chain).replaceAtHead(argThat(contains(
      operation(new PutOperation<>(2L, "Melvin", 0L)),
      operation(new RemoveOperation<>(2L, 0L)),
      operation(new PutIfAbsentOperation<>(2L, "Albin", 0L))
    )));
  }

  @Test
  public void testResolveDoesNotDecodeOtherKeyOperationValues() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
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
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 1),
      new PutOperation<>(1L, "Suresh", 2),
      new PutOperation<>(1L, "Matthew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration(), customCodec);
    resolver.resolve(chain, 1L, 0L);

    assertThat(keySerializer.decodeCount, is(3));
    assertThat(valueSerializer.decodeCount, is(1));
    assertThat(valueSerializer.encodeCount, is(0));
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompactingTwoKeys() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());

    resolver.compact(chain);

    verify(chain).replaceAtHead(argThat(containsInAnyOrder( //@SuppressWarnings("unchecked")
      operation(new PutOperation<>(2L, "Matthew", 0L)),
      operation(new PutOperation<>(1L, "Suresh", 0L))
    )));
  }

  @Test
  public void testCompactEmptyChain() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations();
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);
    verify(chain, never()).replaceAtHead(any());
  }

  @Test
  public void testCompactSinglePut() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L)
    );

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);

    verify(chain, never()).replaceAtHead(any());
  }

  @Test
  public void testCompactMultiplePuts() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);
    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(1L, "Matthew", 0L)))));
  }

  @Test
  public void testCompactSingleRemove() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);
    verify(chain).replaceAtHead(argThat(emptyIterable()));
  }

  @Test
  public void testCompactMultipleRemoves() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new RemoveOperation<>(1L, 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);
    verify(chain).replaceAtHead(argThat(emptyIterable()));
  }

  @Test
  public void testCompactPutAndRemove() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);
    verify(chain).replaceAtHead(argThat(emptyIterable()));
  }

  @Test
  public void testCompactSinglePutIfAbsent() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);
    verify(chain, never()).replaceAtHead(any());
  }

  @Test
  public void testCompactMultiplePutIfAbsents() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutIfAbsentOperation<>(1L, "Suresh", 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);
    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(1L, "Albin", 0L)))));
  }

  @Test
  public void testCompactPutIfAbsentAfterRemove() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);
    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(1L, "Matthew", 0L)))));
  }

  @Test
  public void testCompactForMultipleKeysAndOperations() {
    //create a random mix of operations
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
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
    resolver.compact(chain);
    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(2L, "Albin", 0L)))));
  }

  @Test
  public void testCompactHasCorrectTimeStamp() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0),
      new PutOperation<>(1L, "Albin", 1),
      new RemoveOperation<>(1L, 2),
      new PutOperation<>(1L, "Albin", 3));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    resolver.compact(chain);

    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(1L, "Albin", 3)))));
  }

  @Test
  public void testCompactDecodesOperationValueOnlyOnDemand() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 1),
      new PutOperation<>(1L, "Suresh", 2),
      new PutOperation<>(1L, "Matthew", 3));

    CountingLongSerializer keySerializer = new CountingLongSerializer();
    CountingStringSerializer valueSerializer = new CountingStringSerializer();
    OperationsCodec<Long, String> customCodec = new OperationsCodec<>(keySerializer, valueSerializer);
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration(), customCodec);
    resolver.compact(chain);

    assertThat(keySerializer.decodeCount, is(3)); //Three decodes: one for each operation
    assertThat(keySerializer.encodeCount, is(1)); //One encode from encoding the resolved operation's key

    assertThat(valueSerializer.decodeCount, is(0));
    assertThat(valueSerializer.encodeCount, is(0));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testResolvingTwoKeys() {
    Chain chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(2L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(2L, "Suresh", 0L),
      new PutOperation<>(2L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());

    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);

    assertThat(resolved.get(1L).get(), is("Suresh"));
    assertThat(resolved.get(2L).get(), is("Matthew"));
  }

  @Test
  public void testFullResolveEmptyChain() {
    Chain chain = (new ChainBuilder()).build();
    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);
    assertThat(resolved, is(emptyMap()));
  }

  @Test
  public void testFullResolveSinglePut() {
    Chain chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L)
    );

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);

    assertThat(resolved.get(1L).get(), is("Albin"));
  }

  @Test
  public void testFullResolveMultiplePuts() {
    Chain chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new PutOperation<>(1L, "Suresh", 0L),
      new PutOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);
    assertThat(resolved.get(1L).get(), is("Matthew"));
  }

  @Test
  public void testFullResolveSingleRemove() {
    Chain chain = getEntryFromOperations(new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);
    assertThat(resolved, is(emptyMap()));
  }

  @Test
  public void testFullResolveMultipleRemoves() {
    Chain chain = getEntryFromOperations(
      new RemoveOperation<>(1L, 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);
    assertThat(resolved, is(emptyMap()));
  }

  @Test
  public void testFullResolvePutAndRemove() {
    Chain chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);
    assertThat(resolved, is(emptyMap()));
  }

  @Test
  public void testFullResolveSinglePutIfAbsent() {
    Chain chain = getEntryFromOperations(new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);
    assertThat(resolved.get(1L).get(), is("Matthew"));
  }

  @Test
  public void testFullResolveMultiplePutIfAbsents() {
    Chain chain = getEntryFromOperations(
      new PutIfAbsentOperation<>(1L, "Albin", 0L),
      new PutIfAbsentOperation<>(1L, "Suresh", 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);
    assertThat(resolved.get(1L).get(), is("Albin"));
  }

  @Test
  public void testFullResolvePutIfAbsentAfterRemove() {
    Chain chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0L),
      new RemoveOperation<>(1L, 0L),
      new PutIfAbsentOperation<>(1L, "Matthew", 0L));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);
    assertThat(resolved.get(1L).get(), is("Matthew"));
  }

  @Test
  public void testFullResolveForMultipleKeysAndOperations() {
    //create a random mix of operations
    Chain chain = getEntryFromOperations(
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
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 0L);
    assertThat(resolved.get(2L).get(), is("Albin"));
  }

  @Test
  public void testFullResolveHasCorrectTimeStamp() {
    Chain chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin", 0),
      new PutOperation<>(1L, "Albin", 1),
      new RemoveOperation<>(1L, 2),
      new PutOperation<>(1L, "Albin", 3));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());
    Map<Long, Store.ValueHolder<String>> resolved = resolver.resolveAll(chain, 3);

    assertThat(resolved.get(1L).get(), is("Albin"));
  }

  @Test
  public void testResolveForMultipleOperationHasCorrectIsFirstAndTimeStamp() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin1", 0),
      new PutOperation<>(1L, "Albin2", 1),
      new RemoveOperation<>(1L, 2),
      new PutOperation<>(1L, "AlbinAfterRemove", 3));

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofHours(1)));
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 3);
    assertThat(valueHolder.get(), is("AlbinAfterRemove"));
    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(1L, "AlbinAfterRemove", TimeUnit.HOURS.toMillis(1) + 3)))));
  }

  @Test
  public void testResolveForMultipleOperationHasCorrectIsFirstAndTimeStampWithExpiry() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin1", 0L),
      new PutOperation<>(1L, "Albin2", 1L),
      new PutOperation<>(1L, "Albin3", 2L),
      new PutOperation<>(1L, "Albin4", 3L)
    );

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1L)));
    Store.ValueHolder<String> valueHolder = resolver.resolve(chain, 1L, 3L);

    assertThat(valueHolder.get(), is("Albin4"));
    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(1L, "Albin4", 4L)))));
  }

  @Test
  public void testCompactHasCorrectWithExpiry() {
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(
      new PutOperation<>(1L, "Albin1", 0L),
      new PutOperation<>(1L, "Albin2", 1L),
      new PutOperation<>(1L, "Albin3", 2L),
      new PutOperation<>(1L, "Albin4", 3L)
    );

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1L)));
    resolver.compact(chain);

    verify(chain).replaceAtHead(argThat(contains(operation(new PutOperation<>(1L, "Albin4", 3L)))));
  }

  protected ChainResolver<Long, String> createChainResolver(ExpiryPolicy<? super Long, ? super String> expiryPolicy) {
    return createChainResolver(expiryPolicy, codec);
  }

  @Test
  public void testNonExpiringTimestampIsCleared() throws TimeoutException {
    PutOperation<Long, String> expected = new PutOperation<>(1L, "Albin", 0L);
    ServerStoreProxy.ChainEntry chain = getEntryFromOperations(expected,
      new TimestampOperation<>(1L, 1L)
    );

    ChainResolver<Long, String> resolver = createChainResolver(ExpiryPolicyBuilder.noExpiration());

    assertThat(resolver.resolve(chain, 1L, 2L).get(), is("Albin"));
    verify(chain).replaceAtHead(argThat(contains(operation(expected))));
  }


  @SafeVarargs
  protected final ServerStoreProxy.ChainEntry getEntryFromOperations(Operation<Long, String> ... operations) {
    ChainBuilder chainBuilder = new ChainBuilder();
    for(Operation<Long, String> operation: operations) {
      chainBuilder = chainBuilder.add(codec.encode(operation));
    }
    Chain chain = chainBuilder.build();
    return spy(new ServerStoreProxy.ChainEntry(){

      @Override
      public Iterator<Element> iterator() {
        return chain.iterator();
      }

      @Override
      public void append(ByteBuffer payLoad) throws TimeoutException {
        //nothing
      }

      @Override
      public void replaceAtHead(Chain equivalent) {
        //nothing
      }

      @Override
      public boolean isEmpty() {
        return chain.isEmpty();
      }

      @Override
      public int length() {
        return chain.length();
      }
    });
  }

  protected List<Operation<Long, String>> getOperationsListFromChain(Chain chain) {
    List<Operation<Long, String>> list = new ArrayList<>();
    for (Element element : chain) {
      Operation<Long, String> operation = codec.decode(element.getPayload());
      list.add(operation);
    }
    return list;
  }

  protected Matcher<Element> operation(Operation<?, ?> operation) {
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

  protected Matcher<ByteBuffer> binaryOperation(Operation<?, ?> operation) {
    return new TypeSafeMatcher<ByteBuffer>() {
      @Override
      protected boolean matchesSafely(ByteBuffer item) {
        return operation.equals(codec.decode(item.duplicate()));
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

  <T> T argThat(Matcher<? super T> matches) {
    return ArgumentMatchers.argThat(new ArgumentMatcher<T>() {
      @Override
      public boolean matches(T argument) {
        return matches.matches(argument);
      }
    });
  }
}
