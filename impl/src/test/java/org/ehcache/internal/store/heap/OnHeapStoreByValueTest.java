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

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.exceptions.SerializerException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public class OnHeapStoreByValueTest extends BaseOnHeapStoreTest {

  @Test
  public void testPutNotSerializableValue() throws Exception {
    OnHeapStore<Serializable, Serializable> store = newStore();
    try {
      store.put("key1", new ArrayList<Object>() {{ add(new Object()); }});
      fail();
    } catch (SerializerException se) {
      // expected
    }
  }
  
  @Test
  public void testPutNotSerializableKey() throws Exception {
    OnHeapStore<Serializable, Serializable> store = newStore();
    try {
      store.put(new ArrayList<Object>() {{ add(new Object()); }}, "value");
      fail();
    } catch (SerializerException se) {
      // expected
    }
  }

  @Test
  public void testValueUniqueObject()  throws Exception {
    OnHeapStore<Serializable, Serializable> store = newStore();
    
    String key = "key";
    List<String> value = new ArrayList<String>();
    value.add("value");
    
    store.put(key, (Serializable) value);
    
    // mutate the value -- should not affect cache
    value.clear();
    
    ValueHolder<Serializable> valueHolder = store.get(key);
    if (valueHolder.value() == value || ! valueHolder.value().equals(Collections.singletonList("value"))) {
      throw new AssertionError();
    }
  }
  
  @Test
  public void testKeyUniqueObject() throws Exception {
    OnHeapStore<Serializable, Serializable> store = newStore();
   
    List<String> key = new ArrayList<String>();
    key.add("key");
    String value = "value";
    
    store.put((Serializable) key, value);
    
    // mutate the key -- should not affect cache
    key.clear();
    
    Serializable storeKey = store.iterator().next().getKey();
    if (storeKey == key || ! storeKey.equals(Collections.singletonList("key"))) {
      throw new AssertionError();
    }
  }

  @Override
  protected <K, V> OnHeapStore<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), Eviction.none());
  }

  @Override
  protected <K, V> OnHeapStore<K, V> newStore(EvictionVeto<? super K, ? super V> veto) {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), veto);
  }

  @Override
  protected <K, V> OnHeapStore<K, V> newStore(TimeSource timeSource, Expiry<? super K, ? super V> expiry) {
    return newStore(timeSource, expiry, Eviction.none());
  }

  @Override
  protected <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super K, ? super V> expiry, final EvictionVeto<? super K, ? super V> veto) {
    return new OnHeapStore<K, V>(new Store.Configuration<K, V>() {
      
      @SuppressWarnings("unchecked")
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) Serializable.class;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Class<V> getValueType() {
        return (Class<V>) Serializable.class;
      }

      @Override
      public Comparable<Long> getCapacityConstraint() {
        return null;
      }

      @Override
      public EvictionVeto<? super K, ? super V> getEvictionVeto() {
        return veto;
      }

      @Override
      public EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer() {
        return Eviction.Prioritizer.LRU;
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public Expiry<? super K, ? super V> getExpiry() {
        return expiry;
      }
    }, timeSource, true, new JavaSerializer<K>(getClass().getClassLoader()), new JavaSerializer<V>(getClass().getClassLoader()));
  }

}
