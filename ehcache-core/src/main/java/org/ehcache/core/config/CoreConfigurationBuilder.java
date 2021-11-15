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
package org.ehcache.core.config;

import org.ehcache.Cache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.FluentCacheConfigurationBuilder;
import org.ehcache.config.FluentConfigurationBuilder;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class CoreConfigurationBuilder<B extends CoreConfigurationBuilder<B>> implements FluentConfigurationBuilder<B> {

  private final Map<String, CacheConfiguration<?, ?>> caches;
  private final Collection<ServiceCreationConfiguration<?, ?>> serviceConfigurations;
  private final ClassLoader classLoader;

  /**
   * Create a configuration builder seeded from the given configuration.
   * <p>
   * Calling {@link #build()} on the returned builder will produce a functionally equivalent configuration to
   * {@code seed}.
   *
   * @param seed configuration to duplicate
   * @return a new configuration builder
   */
  protected static CoreConfigurationBuilder<?> newConfigurationBuilder(Configuration seed) {
    return new CoreConfigurationBuilder<>(new CoreConfigurationBuilder<>(new CoreConfigurationBuilder<>(new CoreConfigurationBuilder<>(),
      seed.getCacheConfigurations()), seed.getServiceCreationConfigurations()), seed.getClassLoader());
  }

  protected CoreConfigurationBuilder() {
    this.caches = emptyMap();
    this.serviceConfigurations = emptyList();
    this.classLoader = null;
  }

  protected CoreConfigurationBuilder(CoreConfigurationBuilder<?> builder, Map<String, CacheConfiguration<?, ?>> caches) {
    this.caches = unmodifiableMap(caches);
    this.serviceConfigurations = builder.serviceConfigurations;
    this.classLoader = builder.classLoader;
  }

  protected CoreConfigurationBuilder(CoreConfigurationBuilder<?> builder, Collection<ServiceCreationConfiguration<?, ?>> serviceConfigurations) {
    this.caches = builder.caches;
    this.serviceConfigurations = unmodifiableCollection(serviceConfigurations);
    this.classLoader = builder.classLoader;
  }

  protected CoreConfigurationBuilder(CoreConfigurationBuilder<?> builder, ClassLoader classLoader) {
    this.caches = builder.caches;
    this.serviceConfigurations = builder.serviceConfigurations;
    this.classLoader = classLoader;
  }

  @Override
  public Configuration build() {
    return new DefaultConfiguration(caches, classLoader, serviceConfigurations.toArray(new ServiceCreationConfiguration<?, ?>[serviceConfigurations.size()]));
  }

  @Override
  public CacheConfiguration<?, ?> getCache(String alias) {
    return caches.get(alias);
  }

  @Override
  public B withCache(String alias, CacheConfiguration<?, ?> config) {
    Map<String, CacheConfiguration<?, ?>> newCaches = new HashMap<>(caches);
    newCaches.put(alias, config);
    return newBuilderWith(newCaches);
  }

  @Override
  public B withoutCache(String alias) {
    Map<String, CacheConfiguration<?, ?>> newCaches = new HashMap<>(caches);
    newCaches.remove(alias);
    return newBuilderWith(newCaches);
  }

  @Override
  public B updateCache(String alias, UnaryOperator<FluentCacheConfigurationBuilder<?, ?, ?>> update) {
    CacheConfiguration<?, ?> existing = getCache(alias);
    if (existing == null) {
      throw new IllegalArgumentException("Cache does not exist");
    } else {
      return withCache(alias, update.apply(existing.derive()).build());
    }
  }

  @Override
  public B updateCaches(UnaryOperator<FluentCacheConfigurationBuilder<?, ?, ?>> update) {
    return newBuilderWith(caches.entrySet().stream().collect(
      toMap(Map.Entry::getKey, e -> update.apply(e.getValue().derive()).build())
    ));
  }

  @Override
  public <C extends ServiceCreationConfiguration<?, ?>> Collection<C> getServices(Class<C> configurationType) {
    return serviceConfigurations.stream().filter(service -> configurationType.isAssignableFrom(service.getClass())).map(configurationType::cast).collect(toList());
  }

  @Override
  public B withService(ServiceCreationConfiguration<?, ?> config) {
    List<ServiceCreationConfiguration<?, ?>> newServiceConfigurations = new ArrayList<>(serviceConfigurations);
    newServiceConfigurations.removeIf(other -> !other.compatibleWith(config) || !config.compatibleWith(other));
    newServiceConfigurations.add(config);
    return newBuilderWith(newServiceConfigurations);
  }

  @Override
  public <C extends ServiceCreationConfiguration<?, ?>> B withoutServices(Class<C> clazz, Predicate<? super C> predicate) {
    List<ServiceCreationConfiguration<?, ?>> newServiceConfigurations = new ArrayList<>(serviceConfigurations);
    newServiceConfigurations.removeIf(c -> clazz.isInstance(c) && predicate.test(clazz.cast(c)));
    return newBuilderWith(newServiceConfigurations);
  }

  @Override
  public <R, C extends ServiceCreationConfiguration<?, R>> B updateServices(Class<C> clazz, UnaryOperator<R> update) {
    @SuppressWarnings("unchecked")
    Collection<? extends ServiceCreationConfiguration<?, R>> existing = getServices(clazz);

    if (existing.isEmpty()) {
      throw new IllegalStateException("Cannot updates service configurations. No services exist");
    } else {
      B otherBuilder = withoutServices(clazz);
      for (ServiceCreationConfiguration<?, R> configuration : existing) {
        ServiceCreationConfiguration<?, ?> replacement = configuration.build(update.apply(configuration.derive()));
        if (replacement == null) {
          throw new NullPointerException(configuration.getClass().getSimpleName() + ".build(...) returned a null configuration instance");
        } else {
          otherBuilder = otherBuilder.withService(replacement);
        }
      }
      return otherBuilder;
    }
  }

  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @Override
  public B withClassLoader(ClassLoader classLoader) {
    return newBuilderWith(requireNonNull(classLoader));
  }

  @Override
  public B withDefaultClassLoader() {
    return newBuilderWith((ClassLoader) null);
  }

  @SuppressWarnings("unchecked")
  protected B newBuilderWith(Map<String, CacheConfiguration<?,?>> caches) {
    if (getClass().equals(CoreConfigurationBuilder.class)) {
      return (B) new CoreConfigurationBuilder<>(this, caches);
    } else {
      throw new AssertionError();
    }
  }

  @SuppressWarnings("unchecked")
  protected B newBuilderWith(Collection<ServiceCreationConfiguration<?,?>> serviceConfigurations) {
    if (getClass().equals(CoreConfigurationBuilder.class)) {
      return (B) new CoreConfigurationBuilder<>(this, serviceConfigurations);
    } else {
      throw new AssertionError();
    }
  }

  @SuppressWarnings("unchecked")
  protected B newBuilderWith(ClassLoader classLoader) {
    if (getClass().equals(CoreConfigurationBuilder.class)) {
      return (B) new CoreConfigurationBuilder<>(this, classLoader);
    } else {
      throw new AssertionError();
    }
  }

}
