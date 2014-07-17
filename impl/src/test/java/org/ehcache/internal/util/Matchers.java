/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
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
