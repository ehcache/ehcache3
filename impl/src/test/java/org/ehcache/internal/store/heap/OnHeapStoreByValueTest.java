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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.copy.CopierConfiguration;
import org.ehcache.config.copy.DefaultCopierConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.SerializerException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.Function;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.copy.SerializingCopier;
import org.ehcache.internal.serialization.CompactJavaSerializer;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.spi.cache.AbstractValueHolder;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public class OnHeapStoreByValueTest extends BaseOnHeapStoreTest {

  static class LongCopier implements Copier<Long> {
    int copyForReadCount = 0;
    int copyForWriteCount = 0;

    @Override
    public Long copyForRead(Long obj) {
      copyForReadCount++;
      return obj;
    }

    @Override
    public Long copyForWrite(Long obj) {
      copyForWriteCount++;
      return obj;
    }
  }

  @Test
  public void testKeyCopierCalledOnGetOrComputeIfAbsent() throws Exception {
    LongCopier keyCopier = new LongCopier();
    OnHeapStore<Long, Long> store = newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), Eviction.none(),
        keyCopier, new SerializingCopier<Long>(new CompactJavaSerializer<Long>(ClassLoader.getSystemClassLoader())), 100);

    ValueHolder<Long> computed = store.getOrComputeIfAbsent(1L, new Function<Long, ValueHolder<Long>>() {
      @Override
      public ValueHolder<Long> apply(final Long key) {
        return new AbstractValueHolder<Long>(-1, -1) {
          @Override
          public Long value() {
            return key * 1000L;
          }

          @Override
          protected TimeUnit nativeTimeUnit() {
            return TimeUnit.MILLISECONDS;
          }
        };
      }
    });
    assertThat(computed.value(), is(1000L));
    assertThat(keyCopier.copyForWriteCount, is(1));
    assertThat(keyCopier.copyForReadCount, is(0));
  }

  @Test
  public void testPutNotSerializableValue() throws Exception {
    OnHeapStore<Serializable, Serializable> store = newStore();
    try {
      store.put("key1", new ArrayList<Object>() {{ add(new Object()); }});
      fail();
    } catch (CacheAccessException cae) {
      assertThat(cae.getCause(), instanceOf(SerializerException.class));
    }
  }
  
  @Test
  public void testPutNotSerializableKey() throws Exception {
    OnHeapStore<Serializable, Serializable> store = newStore();
    try {
      store.put(new ArrayList<Object>() {{ add(new Object()); }}, "value");
      fail();
    } catch (CacheAccessException cae) {
      assertThat(cae.getCause(), instanceOf(SerializerException.class));
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
    
    store.put((Serializable)key, value);
    
    // mutate the key -- should not affect cache
    key.clear();
    
    Serializable storeKey = store.iterator().next().getKey();
    if (storeKey == key || ! storeKey.equals(Collections.singletonList("key"))) {
      throw new AssertionError();
    }
  }

  @Test
  public void testStoreByValue() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(false);
    cacheManager.init();

    DefaultCopierConfiguration<String> copierConfiguration = new DefaultCopierConfiguration(
        SerializingCopier.class, CopierConfiguration.Type.VALUE);
    final Cache<Long, String> cache1 = cacheManager.createCache("cache1",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class).withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES))
            .build());
    performAssertions(cache1, true);

    final Cache<Long, String> cache2 = cacheManager.createCache("cache2",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class)
            .add(copierConfiguration)
            .build());
    performAssertions(cache2, false);

    final Cache<Long, String> cache3 = cacheManager.createCache("cache3",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class)
            .build());
    performAssertions(cache3, true);

    cacheManager.close();
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
  protected <K, V> OnHeapStore<K, V> newStore(TimeSource timeSource,
      Expiry<? super K, ? super V> expiry, EvictionVeto<? super K, ? super V> veto) {
    Copier<K> keyCopier = new SerializingCopier<K>(new JavaSerializer<K>(getClass().getClassLoader()));
    Copier<V> valueCopier = new SerializingCopier<V>(new JavaSerializer<V>(getClass().getClassLoader()));
    return newStore(timeSource, expiry, veto, keyCopier, valueCopier, 100);
  }

  private  <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super K, ? super V> expiry, final EvictionVeto<? super K, ? super V> veto,
      final Copier<K> keyCopier, final Copier<V> valueCopier, final int capacity) {
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
      public EvictionVeto<? super K, ? super V> getEvictionVeto() {
        return veto;
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public Expiry<? super K, ? super V> getExpiry() {
        return expiry;
      }

      @Override
      public ResourcePools getResourcePools() {
        return newResourcePoolsBuilder().heap(capacity, EntryUnit.ENTRIES).build();
      }

      @Override
      public Serializer<K> getKeySerializer() {
        return new JavaSerializer<K>(getClass().getClassLoader());
      }

      @Override
      public Serializer<V> getValueSerializer() {
        return new JavaSerializer<V>(getClass().getClassLoader());
      }
    }, timeSource, keyCopier, valueCopier);
  }

  private void performAssertions(Cache<Long, String> cache, boolean same) {
    cache.put(1L, "one");
    String s1 = cache.get(1L);
    String s2 = cache.get(1L);
    String s3 = cache.get(1L);

    assertThat(s1 == s2, is(same));
    assertThat(s2 == s3, is(same));
  }
}
