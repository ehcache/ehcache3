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

package org.ehcache.internal.util;

import org.ehcache.Cache;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 *
 * @author cdennis
 */
public class Matchers {

  public static <K> Matcher<Cache<? super K, ?>> hasKey(final K key) {
    return new TypeSafeMatcher<Cache<? super K, ?>>() {

      @Override
      protected boolean matchesSafely(Cache<? super K, ?> item) {
        return item.containsKey(key);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("cache containing key '").appendValue(key).appendText("'");
      }
    };
  }

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
}
