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
package org.ehcache.management.registry;

import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.terracotta.management.model.context.Context;

import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultManagementRegistryConfiguration implements ManagementRegistryServiceConfiguration {

  private static final AtomicLong COUNTER = new AtomicLong();

  private final Collection<String> tags = new TreeSet<String>();
  private Context context = Context.empty();
  private String collectorExecutorAlias = "collectorExecutor";

  public DefaultManagementRegistryConfiguration() {
    setCacheManagerAlias("cache-manager-" + COUNTER.getAndIncrement());
  }

  public DefaultManagementRegistryConfiguration setCacheManagerAlias(String alias) {
    return setContext(Context.create("cacheManagerName", alias));
  }

  public DefaultManagementRegistryConfiguration setContext(Context context) {
    if (!this.context.contains("cacheManagerName") && !context.contains("cacheManagerName")) {
      throw new IllegalArgumentException("'cacheManagerName' is missing from context");
    }
    this.context = this.context.with(context);
    return this;
  }

  public DefaultManagementRegistryConfiguration setCollectorExecutorAlias(String collectorExecutorAlias) {
    this.collectorExecutorAlias = collectorExecutorAlias;
    return this;
  }

  public DefaultManagementRegistryConfiguration addTags(String... tags) {
    this.tags.addAll(Arrays.asList(tags));
    return this;
  }

  public DefaultManagementRegistryConfiguration addTag(String tag) {
    return addTags(tag);
  }

  @Override
  public Context getContext() {
    return context;
  }

  public String getCacheManagerAlias() {
    return getContext().get("cacheManagerName");
  }

  @Override
  public String getCollectorExecutorAlias() {
    return this.collectorExecutorAlias;
  }

  @Override
  public Collection<String> getTags() {
    return tags;
  }

  @Override
  public Class<ManagementRegistryService> getServiceType() {
    return ManagementRegistryService.class;
  }

  @Override
  public String toString() {
    return "DefaultManagementRegistryConfiguration{" + "context=" + context +
        ", tags=" + tags +
        ", collectorExecutorAlias='" + collectorExecutorAlias + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DefaultManagementRegistryConfiguration that = (DefaultManagementRegistryConfiguration) o;

    if (!tags.equals(that.tags)) return false;
    if (!context.equals(that.context)) return false;
    return collectorExecutorAlias != null ? collectorExecutorAlias.equals(that.collectorExecutorAlias) : that.collectorExecutorAlias == null;

  }

  @Override
  public int hashCode() {
    int result = tags.hashCode();
    result = 31 * result + context.hashCode();
    result = 31 * result + (collectorExecutorAlias != null ? collectorExecutorAlias.hashCode() : 0);
    return result;
  }

}
