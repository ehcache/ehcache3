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

import org.ehcache.core.util.ClassLoading;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.ExposedObject;

import java.util.Collection;
import java.util.Collections;

public abstract class ExposedCacheBinding implements ExposedObject<CacheBinding> {

  protected final CacheBinding cacheBinding;
  private final Context context;

  protected ExposedCacheBinding(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding) {
    this.cacheBinding = cacheBinding;
    this.context = registryConfiguration.getContext().with("cacheName", cacheBinding.getAlias());
  }

  @Override
  public final ClassLoader getClassLoader() {
    ClassLoader classLoader = cacheBinding.getCache().getRuntimeConfiguration().getClassLoader();
    return classLoader == null ? ClassLoading.getDefaultClassLoader() : classLoader;
  }

  @Override
  public final CacheBinding getTarget() {
    return cacheBinding;
  }

  @Override
  public Context getContext() {
    return context;
  }

  @Override
  public Collection<? extends Descriptor> getDescriptors() {
    return Collections.emptyList();
  }

}
