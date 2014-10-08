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

package org.ehcache.internal.store;

import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Ludovic Orban
 */
public class OnHeapStoreBulkMethodsTest {

  @Test
  public void testBulkCompute() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(Arrays.asList(2, 1, 5), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        HashMap<Number, CharSequence> result = new HashMap<Number, CharSequence>();
        for (Map.Entry<? extends Number, ? extends CharSequence> entry : entries) {
          CharSequence value = entry.getValue();
          if (value == null) {
            value = entry.getKey().toString();
          } else {
            value = value + "-modified";
          }
          result.put(entry.getKey(), value);
        }
        return result.entrySet();
      }
    });

    assertThat(result.size(), is(3));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("one-modified"));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two-modified"));
    assertThat(result.get(5).value(), Matchers.<CharSequence>equalTo("5"));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("one-modified"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two-modified"));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    assertThat(store.get(5).value(), Matchers.<CharSequence>equalTo("5"));
  }

  @Test
  public void testBulkComputeIfAbsent() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(Arrays.asList(2, 1, 5), new Function<Iterable<? extends Number>, Map<Number, CharSequence>>() {
      @Override
      public Map<Number, CharSequence> apply(Iterable<? extends Number> numbers) {
        HashMap<Number, CharSequence> result = new HashMap<Number, CharSequence>();
        for (Number number : numbers) {
          result.put(number, number.toString());
        }
        return result;
      }
    });

    assertThat(result.size(), is(1));
    assertThat(result.get(5).value(), Matchers.<CharSequence>equalTo("5"));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("one" +
                                                                    ""));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two" +
                                                                    ""));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    assertThat(store.get(5).value(), Matchers.<CharSequence>equalTo("5"));
  }
}
