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

package org.ehcache.spi.serialization;

import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.config.serializer.DefaultSerializationProviderFactoryConfiguration;
import org.ehcache.internal.classes.ClassInstanceProvider;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ludovic Orban
 */
public class DefaultSerializationProvider extends ClassInstanceProvider<Serializer<?>> implements SerializationProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSerializationProvider.class);

  public DefaultSerializationProvider() {
    super(DefaultSerializationProviderFactoryConfiguration.class, DefaultSerializationProviderConfiguration.class);
  }

  @Override
  public <T> Serializer<T> createSerializer(Class<T> clazz, ClassLoader classLoader, ServiceConfiguration<?>... configs) {
    DefaultSerializationProviderConfiguration config = ServiceLocator.findSingletonAmongst(DefaultSerializationProviderConfiguration.class, configs);
    Serializer<?> serializer = newInstance(buildAlias(clazz, config), config, new Arg(ClassLoader.class, classLoader));
    LOG.info("Serializer for <{}> in <{}> : {}", clazz, (config != null ? config.getAlias() : "any"), serializer);
    return (Serializer<T>) serializer;
  }

  private String buildAlias(Class<?> clazz, DefaultSerializationProviderConfiguration configuration) {
    StringBuilder sb = new StringBuilder();
    if (clazz != null) {
      sb.append(clazz.getName());
    }
    if (configuration != null && configuration.getAlias() != null) {
      if (sb.length() > 0) {
        sb.append("#");
      }
      sb.append(configuration.getAlias());
    }
    return sb.toString();
  }

  @Override
  protected Class<? extends Serializer<?>> getPreconfigured(String alias) {
    Class<? extends Serializer<?>> preconfigured = super.getPreconfigured(alias);
    if (preconfigured != null) {
      return preconfigured;
    }
    String[] split = alias.split("\\#");
    preconfigured = super.getPreconfigured(split[0]);
    if (preconfigured != null) {
      return preconfigured;
    }
    if (split.length > 1) {
      preconfigured = super.getPreconfigured(split[1]);
      if (preconfigured != null) {
        return preconfigured;
      }
    }
    preconfigured = super.getPreconfigured("");
    if (preconfigured != null) {
      return preconfigured;
    }
    return (Class) JavaSerializer.class;
  }

}
