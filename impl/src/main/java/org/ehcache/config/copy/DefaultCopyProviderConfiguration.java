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

package org.ehcache.config.copy;

import org.ehcache.internal.classes.ClassInstanceProviderConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * @author Albin Suresh
 */
public class DefaultCopyProviderConfiguration extends ClassInstanceProviderConfiguration<Copier<?>> implements ServiceCreationConfiguration<CopyProvider> {

  @Override
  public Class<CopyProvider> getServiceType() {
    return CopyProvider.class;
  }

  public <T> DefaultCopyProviderConfiguration addCopierFor(Class<T> clazz, Class<? extends Copier<T>> copierClass) {
    if (clazz == null) {
      throw new NullPointerException("Copy target class cannot be null");
    }
    if (copierClass == null) {
      throw new NullPointerException("Copier class cannot be null");
    }
    String alias = clazz.getName();
    if (getDefaults().containsKey(alias)) {
      throw new IllegalArgumentException("Duplicate copier for class : " + alias);
    }
    getDefaults().put(alias, copierClass);
    return this;
  }
}
