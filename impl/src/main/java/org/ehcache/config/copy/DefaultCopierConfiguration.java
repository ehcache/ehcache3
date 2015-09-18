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

import org.ehcache.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;

/**
 * @author Albin Suresh
 */
public class DefaultCopierConfiguration<T> extends ClassInstanceConfiguration<Copier<T>> implements CopierConfiguration {

  private final Type type;
  private final Copier<T> instance;

  public DefaultCopierConfiguration(Class<? extends Copier<T>> clazz, Type type) {
    super(clazz);
    this.instance = null;
    this.type = type;
  }

  public DefaultCopierConfiguration(Copier<T> instance, Type type) {
    super(null);
    this.instance = instance;
    this.type = type;
  }

  public Copier<T> getInstance() {
    return instance;
  }

  @Override
  public Class<CopyProvider> getServiceType() {
    return CopyProvider.class;
  }

  public Type getType() {
    return type;
  }

}
