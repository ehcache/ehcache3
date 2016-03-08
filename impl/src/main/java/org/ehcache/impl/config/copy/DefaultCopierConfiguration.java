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

package org.ehcache.impl.config.copy;

import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Albin Suresh
 */
public class DefaultCopierConfiguration<T> extends ClassInstanceConfiguration<Copier<T>> implements ServiceConfiguration<CopyProvider> {

  private final Type type;

  public DefaultCopierConfiguration(Class<? extends Copier<T>> clazz, Type type) {
    super(clazz);
    this.type = type;
  }

  public DefaultCopierConfiguration(Copier<T> instance, Type type) {
    super(instance);
    this.type = type;
  }

  DefaultCopierConfiguration(Class<? extends Copier<T>> copierClass) {
    super(copierClass);
    this.type = null;
  }

  @Override
  public Class<CopyProvider> getServiceType() {
    return CopyProvider.class;
  }

  public Type getType() {
    return type;
  }

  /**
   * Copy provider types
   */
  public enum Type {
    KEY,
    VALUE,
  }
}
