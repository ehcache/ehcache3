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

import org.ehcache.internal.serialization.JavaSerializer;
import org.junit.Test;

import java.io.Serializable;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * @author teck
 */
public class SerializedOnHeapKeyTest {
 
  @Test
  public void testLookupOnlyKey() throws Exception {
    LookupOnlyOnHeapKey<String> lookupKey = newLookupOnHeapKey("foo");
    OnHeapKey<String> serializedKey = newSerializedOnHeapKey("foo");
    assertEquals(lookupKey, serializedKey);
    assertEquals(serializedKey, lookupKey);
    assertEquals(serializedKey.hashCode(), lookupKey.hashCode());
    assertEquals(lookupKey.hashCode(), serializedKey.hashCode());
    assertFalse(serializedKey.getActualKeyObject() == lookupKey.getActualKeyObject());
    assertEquals(serializedKey.getActualKeyObject(), lookupKey.getActualKeyObject());
  }

  @Test
  public void testKey() {
    String o = "foo";
    OnHeapKey<?> key1 = newSerializedOnHeapKey(o);
    OnHeapKey<?> key2 = newSerializedOnHeapKey(o);
    assertFalse(key1.getActualKeyObject() == key2.getActualKeyObject());
    assertEquals(key1.getActualKeyObject(), key2.getActualKeyObject());
  }

  @Test
  public void testHashCode() {
    OnHeapKey<Integer> key1 = newSerializedOnHeapKey(10);
    OnHeapKey<Integer> key2 = newSerializedOnHeapKey(10);
    // make sure reading the key multiple times doesn't change the hashcode
    key1.getActualKeyObject();
    key1.getActualKeyObject();
    key2.getActualKeyObject();
    key2.getActualKeyObject();
    assertThat(key1.hashCode(), is(key2.hashCode()));
  }

  @Test
  public void testEquals() {
    OnHeapKey<Integer> key = newSerializedOnHeapKey(10);
    assertThat(newSerializedOnHeapKey(10), equalTo(key));
  }

  @Test
  public void testNotEquals() {
    OnHeapKey<Integer> key = newSerializedOnHeapKey(10);
    assertThat(newSerializedOnHeapKey(101), not(equalTo(key)));
  }

  @Test(expected=NullPointerException.class)
  public void testNullValue() {
    newSerializedOnHeapKey(null);
  }

  private static <K extends Serializable> OnHeapKey<K> newSerializedOnHeapKey(K key) {
    return new SerializedOnHeapKey<K>(key, new JavaSerializer<K>(key.getClass().getClassLoader()));
  }
  
  private static <K extends Serializable> LookupOnlyOnHeapKey<K> newLookupOnHeapKey(K key) {
    return new LookupOnlyOnHeapKey<K>(key);
  }
}
