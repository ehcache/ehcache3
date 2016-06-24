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

package org.ehcache.core.util;

import org.ehcache.ValueSupplier;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.Matchers.equalTo;

/**
 * Matchers
 */
public class Matchers {

  public static <V> Matcher<ValueSupplier<V>> holding(final V value) {
    return holding(equalTo(value));
  }

  public static <V> Matcher<ValueSupplier<V>> holding(final Matcher<? super V> matcher) {
    return new TypeSafeMatcher<ValueSupplier<V>>() {
      @Override
      protected boolean matchesSafely(ValueSupplier<V> item) {
        return matcher.matches(item.value());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("holder containing value ").appendDescriptionOf(matcher);
      }
    };
  }
}
