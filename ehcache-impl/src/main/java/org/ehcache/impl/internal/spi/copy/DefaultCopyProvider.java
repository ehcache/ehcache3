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

package org.ehcache.impl.internal.spi.copy;

import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration.Type;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.impl.internal.classes.ClassInstanceProvider;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Albin Suresh
 */
public class DefaultCopyProvider extends ClassInstanceProvider<Class<?>, DefaultCopierConfiguration<?>, Copier<?>> implements CopyProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultCopyProvider.class);

  @SuppressWarnings("unchecked")
  public DefaultCopyProvider(DefaultCopyProviderConfiguration configuration) {
    super(configuration, (Class) DefaultCopierConfiguration.class);
  }


  @Override
  public <T> Copier<T> createKeyCopier(final Class<T> clazz, Serializer<T> serializer, ServiceConfiguration<?, ?>... configs) {
    return createCopier(Type.KEY, clazz, serializer, configs);
  }

  @Override
  public <T> Copier<T> createValueCopier(final Class<T> clazz, Serializer<T> serializer, ServiceConfiguration<?, ?>... configs) {
    return createCopier(Type.VALUE, clazz, serializer, configs);
  }

  @Override
  public void releaseCopier(Copier<?> copier) throws Exception {
    if (!(copier instanceof SerializingCopier)) {
      releaseInstance(copier);
    }
  }

  private <T> Copier<T> createCopier(Type type, Class<T> clazz,
                                     Serializer<T> serializer, ServiceConfiguration<?, ?>... configs) {
    DefaultCopierConfiguration<T> conf = find(type, configs);
    Copier<T> copier;
    final DefaultCopierConfiguration<?> preConfigured = preconfigured.get(clazz);
    if (conf != null && conf.getClazz().isAssignableFrom(SerializingCopier.class)) {
      if (serializer == null) {
        throw new IllegalStateException("No Serializer configured for type '" + clazz.getName()
                                        + "' which doesn't implement java.io.Serializable");
      }
      copier = new SerializingCopier<>(serializer);
    } else if (conf == null &&  preConfigured != null && preConfigured.getClazz().isAssignableFrom(SerializingCopier.class)) {
      if (serializer == null) {
        throw new IllegalStateException("No Serializer configured for type '" + clazz.getName()
                                        + "' which doesn't implement java.io.Serializable");
      }
      copier = new SerializingCopier<>(serializer);
    } else {
      copier = createCopier(clazz, conf, type);
    }
    LOG.debug("Copier for <{}> : {}", clazz.getName(), copier);
    return copier;
  }

  private <T> Copier<T> createCopier(Class<T> clazz, DefaultCopierConfiguration<T> config, Type type) {
    @SuppressWarnings("unchecked")
    Copier<T> copier = (Copier<T>) newInstance(clazz, config);
    if (copier == null) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      Copier<T> defaultInstance = (Copier<T>) newInstance(clazz, new DefaultCopierConfiguration<>((Class<Copier<T>>) (Class) IdentityCopier.class, type));
      copier = defaultInstance;
    }
    return copier;
  }

  @SuppressWarnings("unchecked")
  private static <T> DefaultCopierConfiguration<T> find(Type type, ServiceConfiguration<?, ?>... serviceConfigurations) {
    DefaultCopierConfiguration<T> result = null;

    @SuppressWarnings("rawtypes")
    Collection<DefaultCopierConfiguration<?>> copierConfigurations = (Collection)
        ServiceUtils.findAmongst(DefaultCopierConfiguration.class, (Object[])serviceConfigurations);
    for (DefaultCopierConfiguration<?> copierConfiguration : copierConfigurations) {
      if (copierConfiguration.getType() == type) {
        if (result != null) {
          throw new IllegalArgumentException("Duplicate " + type + " copier : " + copierConfiguration);
        }
        result = (DefaultCopierConfiguration<T>) copierConfiguration;
      }
    }

    return result;
  }

}
