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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.FluentCacheConfigurationBuilder;
import org.ehcache.config.ResourcePools;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static java.util.Collections.emptyList;
import static org.ehcache.core.config.ExpiryUtils.convertToExpiry;
import static org.ehcache.core.config.ResourcePoolsHelper.createResourcePools;

public class TestCacheConfig<K, V> implements CacheConfiguration<K,V> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final ResourcePools resources;

  public TestCacheConfig(Class<K> keyType, Class<V> valueType) {
    this(keyType, valueType, createResourcePools(100L));
  }

  public TestCacheConfig(Class<K> keyType, Class<V> valueType, ResourcePools resources) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.resources = resources;
  }

  @Override
  public Collection<ServiceConfiguration<?, ?>> getServiceConfigurations() {
    return emptyList();
  }

  @Override
  public Class<K> getKeyType() {
    return keyType;
  }

  @Override
  public Class<V> getValueType() {
    return valueType;
  }

  @Override
  public EvictionAdvisor<? super K, ? super V> getEvictionAdvisor() {
    return Eviction.noAdvice();
  }

  @Override
  public ClassLoader getClassLoader() {
    return null;
  }

  @Override @SuppressWarnings("deprecation")
  public org.ehcache.expiry.Expiry<? super K, ? super V> getExpiry() {
    return convertToExpiry(getExpiryPolicy());
  }

  @Override
  public ExpiryPolicy<? super K, ? super V> getExpiryPolicy() {
    return ExpiryPolicy.NO_EXPIRY;
  }

  @Override
  public ResourcePools getResourcePools() {
    return resources;
  }

  @Override
  public Builder derive() {
    return new Builder();
  }

  public class Builder implements FluentCacheConfigurationBuilder<K, V, Builder> {

    @Override
    public CacheConfiguration<K, V> build() {
      return TestCacheConfig.this;
    }

    @Override
    public <C extends ServiceConfiguration<?, ?>> Collection<C> getServices(Class<C> configurationType) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withService(ServiceConfiguration<?, ?> config) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <C extends ServiceConfiguration<?, ?>> Builder withoutServices(Class<C> clazz, Predicate<? super C> predicate) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <R, C extends ServiceConfiguration<?, R>> Builder updateServices(Class<C> clazz, UnaryOperator<R> update) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withEvictionAdvisor(EvictionAdvisor<? super K, ? super V> evictionAdvisor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withClassLoader(ClassLoader classLoader) {
      return new TestCacheConfig<K, V>(getKeyType(), getValueType(), resources) {
        @Override
        public ClassLoader getClassLoader() {
          return classLoader;
        }
      }.derive();
    }

    @Override
    public Builder withDefaultClassLoader() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withResourcePools(ResourcePools resourcePools) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder updateResourcePools(UnaryOperator<ResourcePools> update) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withExpiry(ExpiryPolicy<? super K, ? super V> expiry) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withLoaderWriter(CacheLoaderWriter<K, V> loaderWriter) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withLoaderWriter(Class<CacheLoaderWriter<K, V>> cacheLoaderWriterClass, Object... arguments) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withoutLoaderWriter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withResilienceStrategy(ResilienceStrategy<K, V> resilienceStrategy) {
      throw new UnsupportedOperationException();
    }

    @Override @SuppressWarnings("rawtypes")
    public Builder withResilienceStrategy(Class<? extends ResilienceStrategy> resilienceStrategyClass, Object... arguments) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withDefaultResilienceStrategy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withKeySerializingCopier() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withValueSerializingCopier() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withKeyCopier(Copier<K> keyCopier) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withKeyCopier(Class<? extends Copier<K>> keyCopierClass) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withoutKeyCopier() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withValueCopier(Copier<V> valueCopier) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withValueCopier(Class<? extends Copier<V>> valueCopierClass) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withoutValueCopier() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withKeySerializer(Serializer<K> keySerializer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withKeySerializer(Class<? extends Serializer<K>> keySerializerClass) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withDefaultKeySerializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withValueSerializer(Serializer<V> valueSerializer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withValueSerializer(Class<? extends Serializer<V>> valueSerializerClass) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withDefaultValueSerializer() {
      throw new UnsupportedOperationException();
    }
  }
}
