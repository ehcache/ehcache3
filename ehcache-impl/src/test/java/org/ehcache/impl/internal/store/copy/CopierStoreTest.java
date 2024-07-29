/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

package org.ehcache.impl.internal.store.copy;

import org.ehcache.Cache;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.store.SimpleTestStore;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.resilience.StoreAccessException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.ehcache.test.MockitoUtil.uncheckedGenericMock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class CopierStoreTest {

  @Test
  public void testGetCopies() throws StoreAccessException {
    Store<String, String> delegate = new SimpleTestStore(singletonMap("foo", "bar"));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    Store.ValueHolder<String> result = store.get("foo");

    assertThat(result.get(), both(is("bar")).and(not(sameInstance("bar"))));

    verifyNoInteractions(keyCopier);
    verify(valueCopier).copyForRead("bar");
  }

  @Test
  public void testContainsKeyNeverCopies() throws StoreAccessException {
    Store<String, String> delegate = new SimpleTestStore(singletonMap("foo", "bar"));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    assertThat(store.containsKey("foo"), is(true));

    verifyNoInteractions(keyCopier, valueCopier);
  }

  @Test
  public void testPutCopiesArguments() throws StoreAccessException {
    Store<String, String> delegate = uncheckedGenericMock(Store.class);

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String foo = "foo";
    String bar = "bar";
    store.put(foo, bar);

    ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
    verify(delegate).put(key.capture(), value.capture());

    assertThat(key.getValue(), not(sameInstance(foo)));
    assertThat(value.getValue(), not(sameInstance(bar)));
  }

  @Test
  public void testPutIfAbsentCopiesArgumentsAndReturn() throws StoreAccessException {
    String baz = "baz";
    Store<String, String> delegate = spy(new SimpleTestStore(singletonMap("foo", baz)));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String foo = "foo";
    String bar = "bar";
    Consumer<Boolean> consumer = put -> {};
    Store.ValueHolder<String> result = store.putIfAbsent(foo, bar, consumer);

    assertThat(result.get(), both(is(baz)).and(not(sameInstance(baz))));

    ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
    verify(delegate).putIfAbsent(key.capture(), value.capture(), same(consumer));

    assertThat(key.getValue(), not(sameInstance(foo)));
    assertThat(value.getValue(), not(sameInstance(bar)));

    verify(keyCopier).copyForWrite(foo);
    verify(valueCopier).copyForWrite(bar);
    verify(valueCopier).copyForRead("baz");
  }

  @Test
  public void testRemoveCopiesNothing() throws StoreAccessException {
    Store<String, String> delegate = new SimpleTestStore(singletonMap("foo", "bar"));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    assertThat(store.remove("foo"), is(true));
    assertThat(delegate.get("foo"), is(nullValue()));

    verifyNoInteractions(keyCopier, valueCopier);
  }

  @Test
  public void testTwoArgRemoveCopiesNothing() throws StoreAccessException {
    Store<String, String> delegate = new SimpleTestStore(singletonMap("foo", "bar"));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String foo = "foo";
    String bar = "bar";
    assertThat(store.remove(foo, bar), is(Store.RemoveStatus.REMOVED));

    verifyNoInteractions(keyCopier, valueCopier);
  }

  @Test
  public void testTwoArgReplaceCopiesValueAndReturn() throws StoreAccessException {
    String baz = "baz";
    Store<String, String> delegate = new SimpleTestStore(singletonMap("foo", baz));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String foo = "foo";
    String bar = "bar";
    assertThat(store.replace(foo, bar).get(), both(is(baz)).and(not(sameInstance(baz))));
    assertThat(delegate.get("foo").get(), both(is(bar)).and(not(sameInstance(bar))));

    verify(valueCopier).copyForWrite(bar);
    verify(valueCopier).copyForRead(baz);
    verifyNoInteractions(keyCopier);
  }

  @Test
  public void testThreeArgReplaceCopiesValueAndReturn() throws StoreAccessException {
    Store<String, String> delegate = new SimpleTestStore(singletonMap("foo", "baz"));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String bar = "bar";
    assertThat(store.replace("foo", "baz", bar), is(Store.ReplaceStatus.HIT));
    assertThat(delegate.get("foo").get(), both(is(bar)).and(not(sameInstance(bar))));

    verify(valueCopier).copyForWrite(bar);
    verifyNoInteractions(keyCopier);
  }

  @Test
  public void testIteratorCopies() throws StoreAccessException {
    String foo = "foo";
    String bar = "bar";
    HashMap<String, String> entries = new HashMap<>();
    entries.put(foo, bar);
    Store<String, String> delegate = new SimpleTestStore(entries);

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = store.iterator();

    Cache.Entry<String, Store.ValueHolder<String>> one = iterator.next();
    assertThat(one.getKey(), both(is(foo)).and(not(sameInstance(foo))));
    assertThat(one.getValue().get(), both(is(bar)).and(not(sameInstance(bar))));

    assertThat(iterator.hasNext(), is(false));

    verify(keyCopier).copyForRead(foo);
    verify(valueCopier).copyForRead(bar);
  }

  @Test
  public void testGetAndPutCopiesArgumentsAndReturn() throws StoreAccessException {
    String bar = "bar";
    Store<String, String> delegate = new SimpleTestStore(singletonMap("foo", bar));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String baz = "baz";
    assertThat(store.getAndPut("foo", baz).get(), both(is(bar)).and(not(sameInstance(bar))));
    assertThat(delegate.get("foo").get(), both(is(baz)).and(not(sameInstance(baz))));

    verify(keyCopier).copyForWrite("foo");
    verify(valueCopier).copyForWrite(baz);
    verify(valueCopier).copyForRead(bar);
  }

  @Test
  public void testGetAndRemoveCopiesReturn() throws StoreAccessException {
    String bar = "bar";
    Store<String, String> delegate = new SimpleTestStore(singletonMap("foo", bar));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    assertThat(store.getAndRemove("foo").get(), both(is(bar)).and(not(sameInstance("bar"))));

    verify(valueCopier).copyForRead(bar);
    verifyNoInteractions(keyCopier);
  }

  @Test
  public void testGetAndComputePassesCopiesAndCopiesReturns() throws StoreAccessException {
    String foo = "foo";
    String bar = "bar";
    Store<String, String> delegate = new SimpleTestStore(singletonMap(foo, bar));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String baz = "baz";
    Store.ValueHolder<String> result = store.getAndCompute("foo", (k, v) -> {
      assertThat(k, both(is(foo)).and(not(sameInstance(foo))));
      assertThat(v, both(is(bar)).and(not(sameInstance(bar))));
      return baz;
    });

    assertThat(result.get(), both(is(bar)).and(not(sameInstance(bar))));
    assertThat(delegate.get("foo").get(), both(is(baz)).and(not(sameInstance(baz))));

    verify(keyCopier).copyForRead(foo);
    verify(valueCopier, times(2)).copyForRead(bar);
    verify(valueCopier).copyForWrite(baz);
  }

  @Test
  public void testComputeAndGetPassesCopiesAndCopiesReturns() throws StoreAccessException {
    String foo = "foo";
    String bar = "bar";
    Store<String, String> delegate = new SimpleTestStore(singletonMap(foo, bar));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String baz = "baz";
    Store.ValueHolder<String> result = store.computeAndGet("foo", (k, v) -> {
      assertThat(k, both(is(foo)).and(not(sameInstance(foo))));
      assertThat(v, both(is(bar)).and(not(sameInstance(bar))));
      return baz;
    }, () -> true, () -> false);

    assertThat(result.get(), both(is(baz)).and(not(sameInstance(baz))));
    assertThat(delegate.get("foo").get(), both(is(baz)).and(not(sameInstance(baz))));

    verify(keyCopier).copyForRead(foo);
    verify(valueCopier).copyForRead(bar);
    verify(valueCopier).copyForWrite(baz);
    verify(valueCopier).copyForRead(baz);
  }

  @Test
  public void testComputeIfAbsentPassesCopiesAndCopiesReturns() throws StoreAccessException {
    Store<String, String> delegate = new SimpleTestStore(emptyMap());

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String foo = "foo";
    String baz = "baz";
    Store.ValueHolder<String> result = store.computeIfAbsent(foo, (k) -> {
      assertThat(k, both(is(foo)).and(not(sameInstance(foo))));
      return baz;
    });

    assertThat(result.get(), both(is(baz)).and(not(sameInstance(baz))));
    assertThat(delegate.get(foo).get(), both(is(baz)).and(not(sameInstance(baz))));

    verify(keyCopier).copyForRead(foo);
    verify(valueCopier).copyForWrite(baz);
    verify(valueCopier).copyForRead(baz);
  }

  @Test
  public void testBulkComputePassesCopiesAndCopiesReturns() throws StoreAccessException {
    String foo = "foo";
    String bar = "bar";
    Store<String, String> delegate = new SimpleTestStore(singletonMap(foo, bar));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String baz = "baz";
    Map<String, Store.ValueHolder<String>> map = store.bulkCompute(singleton(foo), ks -> {
      Map.Entry<? extends String, ? extends String> entry = ks.iterator().next();
      assertThat(entry.getKey(), both(is(foo)).and(not(sameInstance(foo))));
      assertThat(entry.getValue(), both(is(bar)).and(not(sameInstance(bar))));
      return singletonMap(foo, "baz").entrySet();
    });

    Store.ValueHolder<String> result = map.get("foo");
    assertThat(result.get(), both(is(baz)).and(not(sameInstance(baz))));
    assertThat(delegate.get("foo").get(), both(is(baz)).and(not(sameInstance(baz))));

    verify(keyCopier).copyForRead(foo);
    verify(valueCopier).copyForRead(bar);
    verify(valueCopier).copyForWrite(baz);
    verify(valueCopier).copyForRead(baz);
  }

  @Test
  public void testThreeArgBulkComputePassesCopiesAndCopiesReturns() throws StoreAccessException {
    String foo = "foo";
    String bar = "bar";
    Store<String, String> delegate = new SimpleTestStore(singletonMap(foo, bar));

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String baz = "baz";
    Map<String, Store.ValueHolder<String>> map = store.bulkCompute(singleton(foo), ks -> {
      Map.Entry<? extends String, ? extends String> entry = ks.iterator().next();
      assertThat(entry.getKey(), both(is(foo)).and(not(sameInstance(foo))));
      assertThat(entry.getValue(), both(is(bar)).and(not(sameInstance(bar))));
      return singletonMap(foo, "baz").entrySet();
    }, () -> true);

    Store.ValueHolder<String> result = map.get("foo");
    assertThat(result.get(), both(is(baz)).and(not(sameInstance(baz))));
    assertThat(delegate.get("foo").get(), both(is(baz)).and(not(sameInstance(baz))));

    verify(keyCopier).copyForRead(foo);
    verify(valueCopier).copyForRead(bar);
    verify(valueCopier).copyForWrite(baz);
    verify(valueCopier).copyForRead(baz);
  }

  @Test
  public void testBulkComputeIfAbsentPassesCopiesAndCopiesReturns() throws StoreAccessException {
    Store<String, String> delegate = new SimpleTestStore(emptyMap());

    Copier<String> keyCopier = mockCopier();
    Copier<String> valueCopier = mockCopier();

    CopierStore<String, String> store = new CopierStore<>(keyCopier, valueCopier, delegate);

    String foo = "foo";
    String bar = "bar";
    Map<String, Store.ValueHolder<String>> map = store.bulkComputeIfAbsent(singleton(foo), ks -> {
      String key = ks.iterator().next();
      assertThat(key, both(is(foo)).and(not(sameInstance(foo))));
      return singletonMap(foo, bar).entrySet();
    });

    Store.ValueHolder<String> result = map.get("foo");
    assertThat(result.get(), both(is(bar)).and(not(sameInstance(bar))));
    assertThat(delegate.get("foo").get(), both(is(bar)).and(not(sameInstance(bar))));

    verify(keyCopier).copyForRead(foo);
    verify(valueCopier).copyForRead(bar);
    verify(valueCopier).copyForWrite(bar);
  }


  @SuppressWarnings("StringOperationCanBeSimplified")
  private Copier<String> mockCopier() {
    Copier<String> mock = uncheckedGenericMock(Copier.class);
    when(mock.copyForRead(anyString())).thenAnswer(c -> new String(c.<String>getArgument(0)));
    when(mock.copyForWrite(anyString())).thenAnswer(c -> new String(c.<String>getArgument(0)));
    return mock;
  }
}
