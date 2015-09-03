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

package org.ehcache.spi.copy;

import org.ehcache.config.copy.CopierConfiguration;
import org.ehcache.config.copy.DefaultCopierConfiguration;
import org.ehcache.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.internal.classes.ClassInstanceProvider;
import org.ehcache.internal.copy.IdentityCopier;
import org.ehcache.internal.copy.SerializingCopier;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Albin Suresh
 */
public class DefaultCopyProvider extends ClassInstanceProvider<Copier<?>> implements CopyProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultCopyProvider.class);

  public DefaultCopyProvider(DefaultCopyProviderConfiguration configuration) {
    super(configuration, (Class) DefaultCopierConfiguration.class);
  }


  @Override
  public <T> Copier<T> createKeyCopier(final Class<T> clazz, Serializer<T> serializer, ServiceConfiguration<?>... configs) {
    return createCopier(CopierConfiguration.Type.KEY, clazz, serializer, configs);
  }

  @Override
  public <T> Copier<T> createValueCopier(final Class<T> clazz, Serializer<T> serializer, ServiceConfiguration<?>... configs) {
    return createCopier(CopierConfiguration.Type.VALUE, clazz, serializer, configs);
  }

  private <T> Copier<T> createCopier(CopierConfiguration.Type type, Class<T> clazz,
                                     Serializer<T> serializer, ServiceConfiguration<?>... configs) {
    DefaultCopierConfiguration<T> conf = find(type, configs);
    if(conf != null && conf.getClazz().isAssignableFrom(SerializingCopier.class)) {
      return new SerializingCopier<T>(serializer);
    }
    return createCopier(clazz, conf);
  }

  private <T> Copier<T> createCopier(Class<T> clazz, DefaultCopierConfiguration<T> config) {
    String alias = (config != null ? null : clazz.getName());
    Copier<T> copier = (Copier<T>) newInstance(alias, config);
    if (copier == null) {
      LOG.info("No registered copier found for for <{}>. Using the default Identity copier.", clazz.getName());
      copier = new IdentityCopier<T>();
    }
    LOG.info("Copier for <{}> : {}", clazz.getName(), copier);
    return copier;
  }

  @SuppressWarnings("unchecked")
  private static <T> DefaultCopierConfiguration<T> find(CopierConfiguration.Type type, ServiceConfiguration<?>... serviceConfigurations) {
    DefaultCopierConfiguration<T> result = null;

    Collection<DefaultCopierConfiguration> copierConfigurations =
        ServiceLocator.findAmongst(DefaultCopierConfiguration.class, (Object[])serviceConfigurations);
    for (DefaultCopierConfiguration copierConfiguration : copierConfigurations) {
      if (copierConfiguration.getType() == type) {
        if (result != null) {
          throw new IllegalArgumentException("Duplicate " + type + " copier : " + copierConfiguration);
        }
        result = copierConfiguration;
      }
    }

    return result;
  }

}
