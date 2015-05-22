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

package org.ehcache.internal.store.heap;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertSame;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import org.ehcache.internal.SystemTimeSource;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.junit.Test;

/**
 * @author vfunshteyn
 *
 */
public class ByRefOnHeapValueHolderTest {

  @Test
  public void testValue() {
    Object o = "foo";
    ValueHolder<Object> vh1 = newValueHolder(o);
    ValueHolder<Object> vh2 = newValueHolder(o);
    assertSame(vh1.value(), vh2.value());
  }

  @Test
  public void testEquals() {
    long time = SystemTimeSource.INSTANCE.getTimeMillis();
    ValueHolder<Integer> vh = newValueHolder(10, time);
    assertThat(newValueHolder(10, time), is(vh));
  }

  @Test
  public void testNotEqualsOnCreationTime() {
    long time = SystemTimeSource.INSTANCE.getTimeMillis();
    ValueHolder<Integer> vh = newValueHolder(10, time);
    assertThat(newValueHolder(10, time + 1000), not(vh));
  }

  @Test
  public void testNotEquals() {
    long time = SystemTimeSource.INSTANCE.getTimeMillis();
    ValueHolder<Integer> vh = newValueHolder(10, time);
    assertThat(newValueHolder(101, time), not(vh));
  }

  @Test(expected=NullPointerException.class)
  public void testNullValue() {
    newValueHolder(null);
  }

  private static <V> ValueHolder<V> newValueHolder(V value) {
    return newValueHolder(value, SystemTimeSource.INSTANCE.getTimeMillis());
  }

  private static <V> ValueHolder<V> newValueHolder(V value, long creationTimeMillis) {
    return new ByRefOnHeapValueHolder<V>(value, creationTimeMillis);
  }
}
