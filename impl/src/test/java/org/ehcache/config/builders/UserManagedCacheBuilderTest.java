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

package org.ehcache.config.builders;

import org.ehcache.PersistentUserManagedCache;
import org.ehcache.Status;
import org.ehcache.UserManagedCache;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.event.EventType;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.impl.internal.spi.event.DefaultCacheEventListenerProviderTest;
import org.junit.Test;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class UserManagedCacheBuilderTest {

  @Test
  public void testIsExtensible() {

    final UserManagedCacheConfiguration<String, Object, TestUserManagedCache<String, Object>> cfg = builder -> new UserManagedCacheBuilder<String, Object, TestUserManagedCache<String, Object>>(String.class, Object.class) {
      @Override
      TestUserManagedCache<String, Object> build(final ServiceLocator.DependencySet dependencySet) {
        return new TestUserManagedCache<>();
      }
    };

    assertNotNull(cfg);

    final TestUserManagedCache<String, Object> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, Object.class)
        .with(cfg).build();
    assertThat(cache, notNullValue());
    assertThat(cache, instanceOf(TestUserManagedCache.class));
  }

  @Test
  public void testInvalidListenerConfig() {
    try {
      UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, String.class)
        .withEventListeners(CacheEventListenerConfigurationBuilder
            .newEventListenerConfiguration(DefaultCacheEventListenerProviderTest.ListenerObject.class,
                EventType.CREATED,
                EventType.UPDATED)
                .synchronous()
                .unordered()
                .build())
        .build();
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), equalTo("Listeners will not work unless Executors or EventDispatcher is configured."));
    }
  }

  @Test
  public void testTypedCacheWithExpirationPolicy() {
    try (UserManagedCache<String, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, String.class)
      .withExpiry(ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofSeconds(30)))
      .build(true)) {
      assertThat(cache, notNullValue());
    }
  }

  private class TestUserManagedCache<K, V> implements PersistentUserManagedCache<K, V> {

    @Override
    public void destroy() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void init() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Status getStatus() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public V get(final K key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void put(final K key, final V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean containsKey(final K key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void remove(final K key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public V putIfAbsent(K key, V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean remove(K key, V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public V replace(K key, V value) throws NullPointerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public CacheRuntimeConfiguration<K, V> getRuntimeConfiguration() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

  }
}
