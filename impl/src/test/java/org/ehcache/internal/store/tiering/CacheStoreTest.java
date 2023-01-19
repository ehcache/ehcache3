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
package org.ehcache.internal.store.tiering;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class CacheStoreTest {

  @Test
  public void testGetHitsCachingTier() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(cachingTier.getOrComputeIfAbsent(eq(1), any(Function.class))).thenReturn(newValueHolder("one"));

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.get(1).value(), Matchers.<CharSequence>equalTo("one"));

    verify(authoritativeTier, times(0)).getAndFault(any(Number.class));
  }

  @Test
  public void testGetHitsAuthoritativeTier() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    Store.ValueHolder<CharSequence> valueHolder = newValueHolder("one");
    when(authoritativeTier.getAndFault(eq(1))).thenReturn(valueHolder);
    when(cachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).then(new Answer<Store.ValueHolder<CharSequence>>() {
      @Override
      public Store.ValueHolder<CharSequence> answer(InvocationOnMock invocation) throws Throwable {
        Number key = (Number) invocation.getArguments()[0];
        Function<Number, Store.ValueHolder<CharSequence>> function = (Function<Number, Store.ValueHolder<CharSequence>>) invocation.getArguments()[1];
        return function.apply(key);
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.get(1).value(), Matchers.<CharSequence>equalTo("one"));

    verify(cachingTier, times(1)).getOrComputeIfAbsent(eq(1), any(Function.class));
    verify(authoritativeTier, times(1)).getAndFault(any(Number.class));
  }

  @Test
  public void testGetMisses() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.getAndFault(eq(1))).thenReturn(null);
    when(cachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).then(new Answer<Store.ValueHolder<CharSequence>>() {
      @Override
      public Store.ValueHolder<CharSequence> answer(InvocationOnMock invocation) throws Throwable {
        Number key = (Number) invocation.getArguments()[0];
        Function<Number, Store.ValueHolder<CharSequence>> function = (Function<Number, Store.ValueHolder<CharSequence>>) invocation.getArguments()[1];
        return function.apply(key);
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.get(1), is(nullValue()));

    verify(cachingTier, times(1)).getOrComputeIfAbsent(eq(1), any(Function.class));
    verify(authoritativeTier, times(1)).getAndFault(any(Number.class));
  }

  @Test
  public void testPut() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    cacheStore.put(1, "one");

    verify(cachingTier, times(1)).invalidate(eq(1));
    verify(authoritativeTier, times(1)).put(eq(1), eq("one"));
  }

  @Test
  public void testPutIfAbsent_whenAbsent() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.putIfAbsent(1, "one"), is(nullValue()));

    verify(cachingTier, times(1)).invalidate(eq(1));
    verify(authoritativeTier, times(1)).putIfAbsent(eq(1), eq("one"));
  }

  @Test
  public void testPutIfAbsent_whenPresent() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.putIfAbsent(eq(1), eq("one"))).thenReturn(newValueHolder("un"));

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.putIfAbsent(1, "one").value(), Matchers.<CharSequence>equalTo("un"));

    verify(cachingTier, times(0)).invalidate(any(Number.class));
    verify(authoritativeTier, times(1)).putIfAbsent(eq(1), eq("one"));
  }

  @Test
  public void testRemove() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    cacheStore.remove(1);

    verify(cachingTier, times(1)).invalidate(eq(1));
    verify(authoritativeTier, times(1)).remove(eq(1));
  }

  @Test
  public void testRemove2Args_removes() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.remove(eq(1), eq("one"))).thenReturn(true);

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.remove(1, "one"), is(true));

    verify(cachingTier, times(1)).invalidate(eq(1));
    verify(authoritativeTier, times(1)).remove(eq(1), eq("one"));
  }

  @Test
  public void testRemove2Args_doesNotRemove() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.remove(eq(1), eq("one"))).thenReturn(false);

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.remove(1, "one"), is(false));

    verify(cachingTier, times(0)).invalidate(any(Number.class));
    verify(authoritativeTier, times(1)).remove(eq(1), eq("one"));
  }

  @Test
  public void testReplace2Args_replaces() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.replace(eq(1), eq("one"))).thenReturn(newValueHolder("un"));

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.replace(1, "one").value(), Matchers.<CharSequence>equalTo("un"));

    verify(cachingTier, times(1)).invalidate(eq(1));
    verify(authoritativeTier, times(1)).replace(eq(1), eq("one"));
  }

  @Test
  public void testReplace2Args_doesNotReplace() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.replace(eq(1), eq("one"))).thenReturn(null);

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.replace(1, "one"), is(nullValue()));

    verify(cachingTier, times(0)).invalidate(any(Number.class));
    verify(authoritativeTier, times(1)).replace(eq(1), eq("one"));
  }

  @Test
  public void testReplace3Args_replaces() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.replace(eq(1), eq("un"), eq("one"))).thenReturn(true);

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.replace(1, "un", "one"), is(true));

    verify(cachingTier, times(1)).invalidate(eq(1));
    verify(authoritativeTier, times(1)).replace(eq(1), eq("un"), eq("one"));
  }

  @Test
  public void testReplace3Args_doesNotReplace() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.replace(eq(1), eq("un"), eq("one"))).thenReturn(false);

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.replace(1, "un", "one"), is(false));

    verify(cachingTier, times(0)).invalidate(any(Number.class));
    verify(authoritativeTier, times(1)).replace(eq(1), eq("un"), eq("one"));
  }

  @Test
  public void testClear() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    cacheStore.clear();

    verify(cachingTier, times(1)).invalidate();
    verify(authoritativeTier, times(1)).clear();
  }

  @Test
  public void testCompute2Args() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.compute(any(Number.class), any(BiFunction.class))).then(new Answer<Store.ValueHolder<CharSequence>>() {
      @Override
      public Store.ValueHolder<CharSequence> answer(InvocationOnMock invocation) throws Throwable {
        Number key = (Number) invocation.getArguments()[0];
        BiFunction<Number, CharSequence, CharSequence> function = (BiFunction<Number, CharSequence, CharSequence>) invocation.getArguments()[1];
        return newValueHolder(function.apply(key, null));
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.compute(1, new BiFunction<Number, CharSequence, CharSequence>() {
      @Override
      public CharSequence apply(Number number, CharSequence charSequence) {
        return "one";
      }
    }).value(), Matchers.<CharSequence>equalTo("one"));

    verify(cachingTier, times(1)).invalidate(any(Number.class));
    verify(authoritativeTier, times(1)).compute(eq(1), any(BiFunction.class));
  }

  @Test
  public void testCompute3Args() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.compute(any(Number.class), any(BiFunction.class), any(NullaryFunction.class))).then(new Answer<Store.ValueHolder<CharSequence>>() {
      @Override
      public Store.ValueHolder<CharSequence> answer(InvocationOnMock invocation) throws Throwable {
        Number key = (Number) invocation.getArguments()[0];
        BiFunction<Number, CharSequence, CharSequence> function = (BiFunction<Number, CharSequence, CharSequence>) invocation.getArguments()[1];
        return newValueHolder(function.apply(key, null));
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.compute(1, new BiFunction<Number, CharSequence, CharSequence>() {
      @Override
      public CharSequence apply(Number number, CharSequence charSequence) {
        return "one";
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return true;
      }
    }).value(), Matchers.<CharSequence>equalTo("one"));

    verify(cachingTier, times(1)).invalidate(any(Number.class));
    verify(authoritativeTier, times(1)).compute(eq(1), any(BiFunction.class), any(NullaryFunction.class));
  }

  @Test
  public void testComputeIfAbsent_computes() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(cachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).thenAnswer(new Answer<Store.ValueHolder<CharSequence>>() {
      @Override
      public Store.ValueHolder<CharSequence> answer(InvocationOnMock invocation) throws Throwable {
        Number key = (Number) invocation.getArguments()[0];
        Function<Number, Store.ValueHolder<CharSequence>> function = (Function<Number, Store.ValueHolder<CharSequence>>) invocation.getArguments()[1];
        return function.apply(key);
      }
    });
    when(authoritativeTier.computeIfAbsentAndFault(any(Number.class), any(Function.class))).thenAnswer(new Answer<Store.ValueHolder<CharSequence>>() {
      @Override
      public Store.ValueHolder<CharSequence> answer(InvocationOnMock invocation) throws Throwable {
        Number key = (Number) invocation.getArguments()[0];
        Function<Number, CharSequence> function = (Function<Number, CharSequence>) invocation.getArguments()[1];
        return newValueHolder(function.apply(key));
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.computeIfAbsent(1, new Function<Number, CharSequence>() {
      @Override
      public CharSequence apply(Number number) {
        return "one";
      }
    }).value(), Matchers.<CharSequence>equalTo("one"));

    verify(cachingTier, times(1)).getOrComputeIfAbsent(eq(1), any(Function.class));
    verify(authoritativeTier, times(1)).computeIfAbsentAndFault(eq(1), any(Function.class));
  }

  @Test
  public void testComputeIfAbsent_doesNotCompute() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    final Store.ValueHolder<CharSequence> valueHolder = newValueHolder("one");
    when(cachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).thenAnswer(new Answer<Store.ValueHolder<CharSequence>>() {
      @Override
      public Store.ValueHolder<CharSequence> answer(InvocationOnMock invocation) throws Throwable {
        return valueHolder;
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.computeIfAbsent(1, new Function<Number, CharSequence>() {
      @Override
      public CharSequence apply(Number number) {
        return "one";
      }
    }).value(), Matchers.<CharSequence>equalTo("one"));

    verify(cachingTier, times(1)).getOrComputeIfAbsent(eq(1), any(Function.class));
    verify(authoritativeTier, times(0)).computeIfAbsentAndFault(eq(1), any(Function.class));
  }

  @Test
  public void testComputeIfPresent2Args() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.computeIfPresent(any(Number.class), any(BiFunction.class))).then(new Answer<Store.ValueHolder<CharSequence>>() {
      @Override
      public Store.ValueHolder<CharSequence> answer(InvocationOnMock invocation) throws Throwable {
        Number key = (Number) invocation.getArguments()[0];
        BiFunction<Number, CharSequence, CharSequence> function = (BiFunction<Number, CharSequence, CharSequence>) invocation.getArguments()[1];
        return newValueHolder(function.apply(key, null));
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.computeIfPresent(1, new BiFunction<Number, CharSequence, CharSequence>() {
      @Override
      public CharSequence apply(Number number, CharSequence charSequence) {
        return "one";
      }
    }).value(), Matchers.<CharSequence>equalTo("one"));

    verify(cachingTier, times(1)).invalidate(any(Number.class));
    verify(authoritativeTier, times(1)).computeIfPresent(eq(1), any(BiFunction.class));
  }

  @Test
  public void testComputeIfPresent3Args() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.computeIfPresent(any(Number.class), any(BiFunction.class), any(NullaryFunction.class))).then(new Answer<Store.ValueHolder<CharSequence>>() {
      @Override
      public Store.ValueHolder<CharSequence> answer(InvocationOnMock invocation) throws Throwable {
        Number key = (Number) invocation.getArguments()[0];
        BiFunction<Number, CharSequence, CharSequence> function = (BiFunction<Number, CharSequence, CharSequence>) invocation.getArguments()[1];
        return newValueHolder(function.apply(key, null));
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    assertThat(cacheStore.computeIfPresent(1, new BiFunction<Number, CharSequence, CharSequence>() {
      @Override
      public CharSequence apply(Number number, CharSequence charSequence) {
        return "one";
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return true;
      }
    }).value(), Matchers.<CharSequence>equalTo("one"));

    verify(cachingTier, times(1)).invalidate(any(Number.class));
    verify(authoritativeTier, times(1)).computeIfPresent(eq(1), any(BiFunction.class), any(NullaryFunction.class));
  }

  @Test
  public void testBulkCompute2Args() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.bulkCompute(any(Set.class), any(Function.class))).thenAnswer(new Answer<Map<Number, Store.ValueHolder<CharSequence>>>() {
      @Override
      public Map<Number, Store.ValueHolder<CharSequence>> answer(InvocationOnMock invocation) throws Throwable {
        Set<Number> keys = (Set) invocation.getArguments()[0];
        Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>> function = (Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>) invocation.getArguments()[1];

        List<Map.Entry<? extends Number, ? extends CharSequence>> functionArg = new ArrayList<Map.Entry<? extends Number, ? extends CharSequence>>();
        for (Number key : keys) {
          functionArg.add(newMapEntry(key, null));
        }

        Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> functionResult = function.apply(functionArg);

        Map<Number, Store.ValueHolder<CharSequence>> result = new HashMap<Number, Store.ValueHolder<CharSequence>>();
        for (Map.Entry<? extends Number, ? extends CharSequence> entry : functionResult) {
         result.put(entry.getKey(), newValueHolder(entry.getValue()));
        }

        return result;
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    Map<Number, Store.ValueHolder<CharSequence>> result = cacheStore.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2, 3)), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        return new ArrayList<Map.Entry<? extends Number, ? extends CharSequence>>(Arrays.asList(newMapEntry(1, "one"), newMapEntry(2, "two"), newMapEntry(3, "three")));
      }
    });

    assertThat(result.size(), is(3));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3).value(), Matchers.<CharSequence>equalTo("three"));

    verify(cachingTier, times(1)).invalidate(1);
    verify(cachingTier, times(1)).invalidate(2);
    verify(cachingTier, times(1)).invalidate(3);
    verify(authoritativeTier, times(1)).bulkCompute(any(Set.class), any(Function.class));
  }

  @Test
  public void testBulkCompute3Args() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.bulkCompute(any(Set.class), any(Function.class), any(NullaryFunction.class))).thenAnswer(new Answer<Map<Number, Store.ValueHolder<CharSequence>>>() {
      @Override
      public Map<Number, Store.ValueHolder<CharSequence>> answer(InvocationOnMock invocation) throws Throwable {
        Set<Number> keys = (Set) invocation.getArguments()[0];
        Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>> function = (Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>) invocation.getArguments()[1];

        List<Map.Entry<? extends Number, ? extends CharSequence>> functionArg = new ArrayList<Map.Entry<? extends Number, ? extends CharSequence>>();
        for (Number key : keys) {
          functionArg.add(newMapEntry(key, null));
        }

        Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> functionResult = function.apply(functionArg);

        Map<Number, Store.ValueHolder<CharSequence>> result = new HashMap<Number, Store.ValueHolder<CharSequence>>();
        for (Map.Entry<? extends Number, ? extends CharSequence> entry : functionResult) {
          result.put(entry.getKey(), newValueHolder(entry.getValue()));
        }

        return result;
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    Map<Number, Store.ValueHolder<CharSequence>> result = cacheStore.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2, 3)), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        return new ArrayList<Map.Entry<? extends Number, ? extends CharSequence>>(Arrays.asList(newMapEntry(1, "one"), newMapEntry(2, "two"), newMapEntry(3, "three")));
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return true;
      }
    });

    assertThat(result.size(), is(3));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3).value(), Matchers.<CharSequence>equalTo("three"));

    verify(cachingTier, times(1)).invalidate(1);
    verify(cachingTier, times(1)).invalidate(2);
    verify(cachingTier, times(1)).invalidate(3);
    verify(authoritativeTier, times(1)).bulkCompute(any(Set.class), any(Function.class), any(NullaryFunction.class));
  }

  @Test
  public void testBulkComputeIfAbsent() throws Exception {
    CachingTier<Number, CharSequence> cachingTier = mock(CachingTier.class);
    AuthoritativeTier<Number, CharSequence> authoritativeTier = mock(AuthoritativeTier.class);

    when(authoritativeTier.bulkComputeIfAbsent(any(Set.class), any(Function.class))).thenAnswer(new Answer<Map<Number, Store.ValueHolder<CharSequence>>>() {
      @Override
      public Map<Number, Store.ValueHolder<CharSequence>> answer(InvocationOnMock invocation) throws Throwable {
        Set<Number> keys = (Set) invocation.getArguments()[0];
        Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>> function = (Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>) invocation.getArguments()[1];

        List<Map.Entry<? extends Number, ? extends CharSequence>> functionArg = new ArrayList<Map.Entry<? extends Number, ? extends CharSequence>>();
        for (Number key : keys) {
          functionArg.add(newMapEntry(key, null));
        }

        Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> functionResult = function.apply(functionArg);

        Map<Number, Store.ValueHolder<CharSequence>> result = new HashMap<Number, Store.ValueHolder<CharSequence>>();
        for (Map.Entry<? extends Number, ? extends CharSequence> entry : functionResult) {
          result.put(entry.getKey(), newValueHolder(entry.getValue()));
        }

        return result;
      }
    });

    CacheStore<Number, CharSequence> cacheStore = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);


    Map<Number, Store.ValueHolder<CharSequence>> result = cacheStore.bulkComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3)), new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> numbers) {
        return Arrays.asList(newMapEntry(1, "one"), newMapEntry(2, "two"), newMapEntry(3, "three"));
      }
    });

    assertThat(result.size(), is(3));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3).value(), Matchers.<CharSequence>equalTo("three"));

    verify(cachingTier, times(1)).invalidate(1);
    verify(cachingTier, times(1)).invalidate(2);
    verify(cachingTier, times(1)).invalidate(3);
    verify(authoritativeTier, times(1)).bulkComputeIfAbsent(any(Set.class), any(Function.class));
  }

  @Test
  public void CachingTierDoesNotSeeAnyOperationDuringClear() throws CacheAccessException, BrokenBarrierException, InterruptedException {


    final CachingTier<String, String> cachingTier = mock(CachingTier.class);
    final AuthoritativeTier<String, String> authoritativeTier = mock(AuthoritativeTier.class);

    final CacheStore<String, String> cacheStore = new CacheStore<String, String>(cachingTier, authoritativeTier);

    final CyclicBarrier barrier = new CyclicBarrier(2);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        barrier.await();
        barrier.await();
        return null;
      }
    }).when(authoritativeTier).clear();
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          cacheStore.clear();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });

    t.start();
    barrier.await();
    cacheStore.get("foo");
    barrier.await();
    t.join();
    verify(cachingTier, never()).getOrComputeIfAbsent(
        org.mockito.Matchers.<String>any(), org.mockito.Matchers.<Function<String, Store.ValueHolder<String>>>anyObject());
  }

  @Test
  public void AuthoritativeTierNullCheckDuringFlush() throws CacheAccessException, BrokenBarrierException, InterruptedException {


    final CachingTier<String, String> cachingTier = mock(CachingTier.class);
    final AuthoritativeTier<String, String> authoritativeTier = mock(AuthoritativeTier.class);

    final CacheStore<String, String> cacheStore = new CacheStore<String, String>(cachingTier, authoritativeTier);

    final CyclicBarrier barrier = new CyclicBarrier(2);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        barrier.await();
        barrier.await();
        return null;
      }
    }).when(authoritativeTier).clear();
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          cacheStore.clear();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });

    t.start();
    barrier.await();
    cacheStore.get("foo");
    barrier.await();
    t.join();
    verify(stringAuthoritativeTier, never()).flush("foo", null);
  }

  public Map.Entry<? extends Number, ? extends CharSequence> newMapEntry(Number key, CharSequence value) {
    return new AbstractMap.SimpleEntry<Number, CharSequence>(key, value);
  }

  public Store.ValueHolder<CharSequence> newValueHolder(final CharSequence v) {
    return new Store.ValueHolder<CharSequence>() {

      @Override
      public CharSequence value() {
        return v;
      }

      @Override
      public long creationTime(TimeUnit unit) {
        return 0;
      }

      @Override
      public long expirationTime(TimeUnit unit) {
        return 0;
      }

      @Override
      public boolean isExpired(long expirationTime, TimeUnit unit) {
        return false;
      }

      @Override
      public long lastAccessTime(TimeUnit unit) {
        return 0;
      }

      @Override
      public float hitRate(TimeUnit unit) {
        return 0;
      }

      @Override
      public long getId() {
        throw new UnsupportedOperationException("Implement me!");
      }
    };
  }

}
