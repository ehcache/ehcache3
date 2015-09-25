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
import org.ehcache.internal.classes.ClassInstanceConfiguration;
import org.ehcache.internal.classes.ClassInstanceProvider;
import org.ehcache.internal.copy.IdentityCopier;
import org.ehcache.internal.copy.SerializingCopier;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
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
    Copier<T> copier;
    final ClassInstanceConfiguration<Copier<?>> preConfigured = preconfigured.get(clazz.getName());
    if (conf != null && conf.getInstance() != null) {
      copier = conf.getInstance();
    } else if (conf != null && conf.getClazz().isAssignableFrom(SerializingCopier.class)) {
      if (serializer == null) {
        throw new IllegalStateException("No Serializer configured for type '" + clazz.getName()
                                        + "' which doesn't implement java.io.Serializable");
      }
      copier = new SerializingCopier<T>(serializer);
    } else if (conf == null &&  preConfigured != null && preConfigured.getClazz().isAssignableFrom(SerializingCopier.class)) {
      if (serializer == null) {
        throw new IllegalStateException("No Serializer configured for type '" + clazz.getName()
                                        + "' which doesn't implement java.io.Serializable");
      }
      copier = new SerializingCopier<T>(serializer);
    } else {
      copier = createCopier(clazz, conf);
    }
    LOG.info("Copier for <{}> : {}", clazz.getName(), copier);
    return copier;
  }

  private <T> Copier<T> createCopier(Class<T> clazz, DefaultCopierConfiguration<T> config) {
    String alias = (config != null ? null : clazz.getName());
    Copier<T> copier = (Copier<T>) newInstance(alias, config);
    if (copier == null) {
      copier = new IdentityCopier<T>();
    }
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
