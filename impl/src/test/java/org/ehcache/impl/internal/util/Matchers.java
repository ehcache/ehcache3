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

package org.ehcache.impl.internal.util;

import org.ehcache.Cache;
import org.ehcache.event.EventType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.function.Supplier;

/**
 *
 * @author cdennis
 */
public class Matchers {

  public static <K, V> Matcher<Cache<? super K, ? super V>> hasEntry(final K key, final V value) {
    return new TypeSafeMatcher<Cache<? super K, ? super V>>() {

      @Override
      protected boolean matchesSafely(Cache<? super K, ? super V> item) {
        return value.equals(item.get(key));
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("cache containing entry {").appendValue(key).appendText(", ").appendValue(value).appendText("}");
      }
    };
  }

  public static <V> Matcher<Store.ValueHolder<V>> valueHeld(final V value) {
    return new TypeSafeMatcher<Store.ValueHolder<V>>() {
      @Override
      protected boolean matchesSafely(Store.ValueHolder<V> item) {
        return item.get().equals(value);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("value holder containing value '").appendValue(value).appendText("'");
      }
    };
  }

  public static <V> Matcher<Supplier<V>> holding(final V value) {
    return new TypeSafeMatcher<Supplier<V>>() {
      @Override
      protected boolean matchesSafely(Supplier<V> item) {
        return item.get().equals(value);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("holder containing value '").appendValue(value).appendText("'");
      }
    };
  }

  public static <K, V> Matcher<StoreEvent<K, V>> eventOfType(final EventType type) {
    return new TypeSafeMatcher<StoreEvent<K, V>>() {
      @Override
      protected boolean matchesSafely(StoreEvent<K, V> item) {
        return item.getType().equals(type);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("event of type '").appendValue(type).appendText("'");
      }
    };
  }
}
