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
package org.ehcache.management.providers;

import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.management.annotations.Named;
import org.ehcache.management.registry.CacheBinding;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.stats.Statistic;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Mathieu Carbou
 */
public abstract class CacheBindingManagementProviderSkeleton<V> implements ManagementProvider<CacheBinding> {

  protected final ConcurrentMap<CacheBinding, V> managedObjects = new ConcurrentHashMap<CacheBinding, V>();

  private final String cacheManagerAlias;
  private final String name;

  protected CacheBindingManagementProviderSkeleton(String cacheManagerAlias) {
    this.cacheManagerAlias = cacheManagerAlias;
    Named named = getClass().getAnnotation(Named.class);
    this.name = named == null ? getClass().getSimpleName() : named.value();
  }

  @Override
  public final Class<CacheBinding> managedType() {
    return CacheBinding.class;
  }

  @Override
  public final void register(CacheBinding managedObject) {
    managedObjects.putIfAbsent(managedObject, createManagedObject(managedObject));
  }

  @Override
  public final void unregister(CacheBinding managedObject) {
    V managed = managedObjects.remove(managedObject);
    if (managed != null) {
      close(managedObject, managed);
    }
  }

  @Override
  public final CapabilityContext getCapabilityContext() {
    return new CapabilityContext(Arrays.asList(new CapabilityContext.Attribute("cacheManagerName", true), new CapabilityContext.Attribute("cacheName", true)));
  }

  @Override
  public final String getCapabilityName() {
    return name;
  }

  @Override
  public final Capability getCapability() {
    String name = getCapabilityName();
    CapabilityContext context = getCapabilityContext();
    Collection<Descriptor> descriptors = getDescriptors();
    return createCapability(name, context, descriptors);
  }

  @Override
  public final boolean supports(Map<String, String> context) {
    return findManagedObject(context) != null;
  }

  @Override
  public void close() {
    for (CacheBinding cacheBinding : managedObjects.keySet()) {
      unregister(cacheBinding);
    }
  }

  protected final Map.Entry<CacheBinding, V> findManagedObject(Map<String, String> context) {
    String cacheManagerName = context.get("cacheManagerName");
    String cacheName = context.get("cacheName");
    if (!this.cacheManagerAlias.equals(cacheManagerName)) {
      return null;
    }
    for (Map.Entry<CacheBinding, V> entry : managedObjects.entrySet()) {
      if (entry.getKey().getAlias().equals(cacheName)) {
        return entry;
      }
    }
    return null;
  }

  @Override
  public Collection<Statistic<?>> collectStatistics(Map<String, String> context, String[] statisticNames) {
    throw new UnsupportedOperationException("Not a statistics provider : " + getCapabilityName());
  }

  @Override
  public Object callAction(Map<String, String> context, String methodName, String[] argClassNames, Object[] args) {
    throw new UnsupportedOperationException("Not an action provider : " + getCapabilityName());
  }

  protected abstract Capability createCapability(String name, CapabilityContext context, Collection<Descriptor> descriptors);

  protected void close(CacheBinding cacheBinding, V managed) {

  }

  protected abstract V createManagedObject(CacheBinding cacheBinding);

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("{");
    sb.append("cacheManagerAlias='").append(cacheManagerAlias).append('\'');
    sb.append(", name='").append(name).append('\'');
    sb.append(", managedObjects=").append(managedObjects.keySet());
    sb.append('}');
    return sb.toString();
  }
}
