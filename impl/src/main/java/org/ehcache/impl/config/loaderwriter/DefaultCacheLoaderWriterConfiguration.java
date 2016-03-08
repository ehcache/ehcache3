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

package org.ehcache.impl.config.loaderwriter;

import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
* @author Alex Snaps
*/
public class DefaultCacheLoaderWriterConfiguration extends ClassInstanceConfiguration<CacheLoaderWriter<?, ?>> implements ServiceConfiguration<CacheLoaderWriterProvider> {

  public DefaultCacheLoaderWriterConfiguration(final Class<? extends CacheLoaderWriter<?, ?>> clazz, Object... arguments) {
    super(clazz, arguments);
  }

  public DefaultCacheLoaderWriterConfiguration(CacheLoaderWriter<?, ?> loaderWriter) {
    super(loaderWriter);
  }

  @Override
  public Class<CacheLoaderWriterProvider> getServiceType() {
    return CacheLoaderWriterProvider.class;
  }

}
