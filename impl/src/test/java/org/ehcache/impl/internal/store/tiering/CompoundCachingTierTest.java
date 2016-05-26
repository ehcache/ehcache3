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
package org.ehcache.impl.internal.store.tiering;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.spi.store.tiering.HigherCachingTier;
import org.ehcache.core.spi.store.tiering.LowerCachingTier;
import org.ehcache.impl.internal.util.UnmatchedResourceType;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.EMPTY_LIST;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class CompoundCachingTierTest {

  @Test
  public void testGetOrComputeIfAbsentComputesWhenBothTiersEmpty() throws Exception {
    HigherCachingTier<String, String> higherTier = mock(HigherCachingTier.class);
    LowerCachingTier<String, String> lowerTier = mock(LowerCachingTier.class);
    final Store.ValueHolder<String> valueHolder = mock(Store.ValueHolder.class);

    final ArgumentCaptor<Function> functionArg = ArgumentCaptor.forClass(Function.class);
    final ArgumentCaptor<String> keyArg = ArgumentCaptor.forClass(String.class);
    when(higherTier.getOrComputeIfAbsent(keyArg.capture(), functionArg.capture())).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return functionArg.getValue().apply(keyArg.getValue());
      }
    });
    when(lowerTier.getAndRemove(anyString())).thenReturn(null);


    CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<String, String>(higherTier, lowerTier);

    final AtomicBoolean computed = new AtomicBoolean(false);

    assertThat(compoundCachingTier.getOrComputeIfAbsent("1", new Function<String, Store.ValueHolder<String>>() {
      @Override
      public Store.ValueHolder<String> apply(String s) {
        computed.set(true);
        return valueHolder;
      }
    }), is(valueHolder));
    assertThat(computed.get(), is(true));
  }

  @Test
  public void testGetOrComputeIfAbsentDoesNotComputesWhenHigherTierContainsValue() throws Exception {
    HigherCachingTier<String, String> higherTier = mock(HigherCachingTier.class);
    LowerCachingTier<String, String> lowerTier = mock(LowerCachingTier.class);
    final Store.ValueHolder<String> valueHolder = mock(Store.ValueHolder.class);

    when(higherTier.getOrComputeIfAbsent(anyString(), any(Function.class))).thenReturn(valueHolder);
    when(lowerTier.getAndRemove(anyString())).thenThrow(AssertionError.class);


    CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<String, String>(higherTier, lowerTier);

    final AtomicBoolean computed = new AtomicBoolean(false);

    assertThat(compoundCachingTier.getOrComputeIfAbsent("1", new Function<String, Store.ValueHolder<String>>() {
      @Override
      public Store.ValueHolder<String> apply(String s) {
        computed.set(true);
        return valueHolder;
      }
    }), is(valueHolder));
    assertThat(computed.get(), is(false));
  }

  @Test
  public void testGetOrComputeIfAbsentDoesNotComputesWhenLowerTierContainsValue() throws Exception {
    HigherCachingTier<String, String> higherTier = mock(HigherCachingTier.class);
    LowerCachingTier<String, String> lowerTier = mock(LowerCachingTier.class);
    final Store.ValueHolder<String> valueHolder = mock(Store.ValueHolder.class);

    final ArgumentCaptor<Function> functionArg = ArgumentCaptor.forClass(Function.class);
    final ArgumentCaptor<String> keyArg = ArgumentCaptor.forClass(String.class);
    when(higherTier.getOrComputeIfAbsent(keyArg.capture(), functionArg.capture())).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return functionArg.getValue().apply(keyArg.getValue());
      }
    });
    when(lowerTier.getAndRemove(anyString())).thenReturn(valueHolder);


    CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<String, String>(higherTier, lowerTier);

    final AtomicBoolean computed = new AtomicBoolean(false);

    assertThat(compoundCachingTier.getOrComputeIfAbsent("1", new Function<String, Store.ValueHolder<String>>() {
      @Override
      public Store.ValueHolder<String> apply(String s) {
        computed.set(true);
        return valueHolder;
      }
    }), is(valueHolder));
    assertThat(computed.get(), is(false));
  }

  @Test
  public void testGetOrComputeIfAbsentComputesWhenLowerTierExpires() throws Exception {
    HigherCachingTier<String, String> higherTier = mock(HigherCachingTier.class);
    final LowerCachingTier<String, String> lowerTier = mock(LowerCachingTier.class);
    final Store.ValueHolder<String> originalValueHolder = mock(Store.ValueHolder.class);
    final Store.ValueHolder<String> newValueHolder = mock(Store.ValueHolder.class);

    final ArgumentCaptor<Function> functionArg = ArgumentCaptor.forClass(Function.class);
    final ArgumentCaptor<String> keyArg = ArgumentCaptor.forClass(String.class);
    when(higherTier.getOrComputeIfAbsent(keyArg.capture(), functionArg.capture())).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return functionArg.getValue().apply(keyArg.getValue());
      }
    });
    final ArgumentCaptor<CachingTier.InvalidationListener> invalidationListenerArg = ArgumentCaptor.forClass(CachingTier.InvalidationListener.class);
    doNothing().when(lowerTier).setInvalidationListener(invalidationListenerArg.capture());
    when(lowerTier.getAndRemove(anyString())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        invalidationListenerArg.getValue().onInvalidation(invocation.getArguments()[0], originalValueHolder);
        return null;
      }
    });

    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<Store.ValueHolder<String>>();

    CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<String, String>(higherTier, lowerTier);
    compoundCachingTier.setInvalidationListener(new CachingTier.InvalidationListener<String, String>() {
      @Override
      public void onInvalidation(String key, Store.ValueHolder<String> valueHolder) {
        invalidated.set(valueHolder);
      }
    });

    final AtomicBoolean computed = new AtomicBoolean(false);

    assertThat(compoundCachingTier.getOrComputeIfAbsent("1", new Function<String, Store.ValueHolder<String>>() {
      @Override
      public Store.ValueHolder<String> apply(String s) {
        computed.set(true);
        return newValueHolder;
      }
    }), is(newValueHolder));
    assertThat(computed.get(), is(true));
    assertThat(invalidated.get(), is(originalValueHolder));
  }

  @Test
  public void testInvalidateNoArg() throws Exception {
    HigherCachingTier<String, String> higherTier = mock(HigherCachingTier.class);
    LowerCachingTier<String, String> lowerTier = mock(LowerCachingTier.class);

    CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<String, String>(higherTier, lowerTier);

    compoundCachingTier.clear();
    verify(higherTier, times(1)).clear();
    verify(lowerTier, times(1)).clear();
  }

  @Test
  public void testInvalidateWhenNoValueDoesNotFireListener() throws Exception {
    HigherCachingTier<String, String> higherTier = mock(HigherCachingTier.class);
    LowerCachingTier<String, String> lowerTier = mock(LowerCachingTier.class);

    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<Store.ValueHolder<String>>();

    CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<String, String>(higherTier, lowerTier);
    compoundCachingTier.setInvalidationListener(new CachingTier.InvalidationListener<String, String>() {
      @Override
      public void onInvalidation(String key, Store.ValueHolder<String> valueHolder) {
        invalidated.set(valueHolder);
      }
    });

    compoundCachingTier.invalidate("1");

    assertThat(invalidated.get(), is(nullValue()));
  }

  @Test
  public void testInvalidateWhenValueInLowerTierFiresListener() throws Exception {
    HigherCachingTier<String, String> higherTier = mock(HigherCachingTier.class);
    LowerCachingTier<String, String> lowerTier = mock(LowerCachingTier.class);

    final Store.ValueHolder<String> valueHolder = mock(Store.ValueHolder.class);
    final AtomicReference<Store.ValueHolder<String>> higherTierValueHolder = new AtomicReference<Store.ValueHolder<String>>();
    final AtomicReference<Store.ValueHolder<String>> lowerTierValueHolder = new AtomicReference<Store.ValueHolder<String>>(valueHolder);

    final ArgumentCaptor<CachingTier.InvalidationListener> higherTierInvalidationListenerArg = ArgumentCaptor.forClass(CachingTier.InvalidationListener.class);
    doNothing().when(higherTier).setInvalidationListener(higherTierInvalidationListenerArg.capture());
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        higherTierInvalidationListenerArg.getValue().onInvalidation(invocation.getArguments()[0], higherTierValueHolder.getAndSet(null));
        ((Function) invocation.getArguments()[1]).apply(higherTierValueHolder.get());
        return null;
      }
    }).when(higherTier).silentInvalidate(anyString(), any(Function.class));

    final ArgumentCaptor<Function> functionArg = ArgumentCaptor.forClass(Function.class);
    final ArgumentCaptor<String> keyArg = ArgumentCaptor.forClass(String.class);
    when(lowerTier.installMapping(keyArg.capture(), functionArg.capture())).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return lowerTierValueHolder.get();
      }
    });

    final ArgumentCaptor<CachingTier.InvalidationListener> lowerTierInvalidationListenerArg = ArgumentCaptor.forClass(CachingTier.InvalidationListener.class);
    doNothing().when(lowerTier).setInvalidationListener(lowerTierInvalidationListenerArg.capture());
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        lowerTierInvalidationListenerArg.getValue().onInvalidation(invocation.getArguments()[0], lowerTierValueHolder.getAndSet(null));
        return null;
      }
    }).when(lowerTier).invalidate(anyString());

    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<Store.ValueHolder<String>>();

    CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<String, String>(higherTier, lowerTier);
    compoundCachingTier.setInvalidationListener(new CachingTier.InvalidationListener<String, String>() {
      @Override
      public void onInvalidation(String key, Store.ValueHolder<String> valueHolder) {
        invalidated.set(valueHolder);
      }
    });


    compoundCachingTier.invalidate("1");

    assertThat(invalidated.get(), is(valueHolder));
    assertThat(higherTierValueHolder.get(), is(nullValue()));
    assertThat(lowerTierValueHolder.get(), is(nullValue()));
  }

  @Test
  public void testInvalidateWhenValueInHigherTierFiresListener() throws Exception {
    HigherCachingTier<String, String> higherTier = mock(HigherCachingTier.class);
    LowerCachingTier<String, String> lowerTier = mock(LowerCachingTier.class);

    final Store.ValueHolder<String> valueHolder = mock(Store.ValueHolder.class);
    final AtomicReference<Store.ValueHolder<String>> higherTierValueHolder = new AtomicReference<Store.ValueHolder<String>>(valueHolder);
    final AtomicReference<Store.ValueHolder<String>> lowerTierValueHolder = new AtomicReference<Store.ValueHolder<String>>();

    final ArgumentCaptor<CachingTier.InvalidationListener> higherTierInvalidationListenerArg = ArgumentCaptor.forClass(CachingTier.InvalidationListener.class);
    doNothing().when(higherTier).setInvalidationListener(higherTierInvalidationListenerArg.capture());
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        higherTierInvalidationListenerArg.getValue().onInvalidation(invocation.getArguments()[0], higherTierValueHolder.getAndSet(null));
        ((Function) invocation.getArguments()[1]).apply(higherTierValueHolder.get());
        return null;
      }
    }).when(higherTier).silentInvalidate(anyString(), any(Function.class));
    final ArgumentCaptor<Function> functionArg = ArgumentCaptor.forClass(Function.class);
    final ArgumentCaptor<String> keyArg = ArgumentCaptor.forClass(String.class);
    when(lowerTier.installMapping(keyArg.capture(), functionArg.capture())).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object apply = functionArg.getValue().apply(keyArg.getValue());
        lowerTierValueHolder.set((Store.ValueHolder<String>) apply);
        return apply;
      }
    });

    final ArgumentCaptor<CachingTier.InvalidationListener> lowerTierInvalidationListenerArg = ArgumentCaptor.forClass(CachingTier.InvalidationListener.class);
    doNothing().when(lowerTier).setInvalidationListener(lowerTierInvalidationListenerArg.capture());
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        lowerTierInvalidationListenerArg.getValue().onInvalidation(invocation.getArguments()[0], lowerTierValueHolder.getAndSet(null));
        return null;
      }
    }).when(lowerTier).invalidate(anyString());

    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<Store.ValueHolder<String>>();

    CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<String, String>(higherTier, lowerTier);
    compoundCachingTier.setInvalidationListener(new CachingTier.InvalidationListener<String, String>() {
      @Override
      public void onInvalidation(String key, Store.ValueHolder<String> valueHolder) {
        invalidated.set(valueHolder);
      }
    });


    compoundCachingTier.invalidate("1");

    assertThat(invalidated.get(), is(valueHolder));
    assertThat(higherTierValueHolder.get(), is(nullValue()));
    assertThat(lowerTierValueHolder.get(), is(nullValue()));
  }

  @Test
  public void testInvalidateAllCoversBothTiers() throws Exception {
    HigherCachingTier<String, String> higherTier = mock(HigherCachingTier.class);
    LowerCachingTier<String, String> lowerTier = mock(LowerCachingTier.class);

    CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<String, String>(higherTier, lowerTier);

    compoundCachingTier.invalidateAll();

    verify(higherTier).silentInvalidateAll(any(BiFunction.class));
    verify(lowerTier).invalidateAll();
  }

  @Test
  public void testRankCachingTier() throws Exception {
    CompoundCachingTier.Provider provider = new CompoundCachingTier.Provider();
    HashSet<ResourceType<?>> resourceTypes = new HashSet<ResourceType<?>>();
    resourceTypes.addAll(EnumSet.of(ResourceType.Core.HEAP, ResourceType.Core.OFFHEAP));
    assertThat(provider.rankCachingTier(resourceTypes, EMPTY_LIST), is(2));

    resourceTypes.clear();
    resourceTypes.add(new UnmatchedResourceType());
    assertThat(provider.rankCachingTier(resourceTypes, EMPTY_LIST), is(0));
  }
}
