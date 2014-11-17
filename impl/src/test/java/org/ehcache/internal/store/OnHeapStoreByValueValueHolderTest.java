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

import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.junit.Test;

import java.io.Serializable;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * @author vfunshteyn
 *
 */
public class OnHeapStoreByValueValueHolderTest {

  @Test
  public void testValue() {
    String o = "foo";
    ValueHolder<?> vh1 = newValueHolder(o);
    ValueHolder<?> vh2 = newValueHolder(o);
    assertFalse(vh1.value() == vh2.value());
    assertEquals(vh1.value(), vh2.value());
  }

  @Test
  public void testHashCode() {
    ValueHolder<Integer> vh1 = newValueHolder(10);
    ValueHolder<Integer> vh2 = newValueHolder(10);
    // make sure reading the value multiple times doesn't change the hashcode
    vh1.value();
    vh1.value();
    vh2.value();
    vh2.value();
    assertThat(vh1.hashCode(), is(vh2.hashCode()));
  }

  @Test
  public void testEquals() {
    ValueHolder<Integer> vh = newValueHolder(10);
    assertThat(newValueHolder(10), equalTo(vh));
  }

  @Test
  public void testNotEquals() {
    ValueHolder<Integer> vh = newValueHolder(10);
    assertThat(newValueHolder(101), not(equalTo(vh)));
  }

  @Test(expected=NullPointerException.class)
  public void testNullValue() {
    newValueHolder(null);
  }

  private static <V extends Serializable> TimeStampedOnHeapByValueValueHolder<V> newValueHolder(V value) {
    return new TimeStampedOnHeapByValueValueHolder<V>(value, new JavaSerializer<V>(), System.currentTimeMillis(), TimeStampedOnHeapValueHolder.NO_EXPIRE);
  }
}
