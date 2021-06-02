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

package org.ehcache.xml.provider;

import org.ehcache.config.Configuration;
import org.ehcache.config.FluentConfigurationBuilder;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.CoreServiceCreationConfigurationParser;
import org.ehcache.xml.model.ConfigType;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;

class SimpleCoreServiceCreationConfigurationParser<T, U extends ServiceCreationConfiguration<?, ?>> implements CoreServiceCreationConfigurationParser {

  private final Class<U> configType;

  private final Function<ConfigType, T> getter;
  private final BiConsumer<ConfigType, T> setter;

  private final Parser<T, U> parser;
  private final Function<U, T> unparser;

  private final BinaryOperator<T> merger;

  SimpleCoreServiceCreationConfigurationParser(Class<U> configType,
                                               Function<ConfigType, T> getter, BiConsumer<ConfigType, T> setter,
                                               Function<T, U> parser, Function<U, T> unparser) {
    this(configType, getter, setter, (config, loader) -> parser.apply(config), unparser, (a, b) -> { throw new IllegalStateException(); });
  }

  SimpleCoreServiceCreationConfigurationParser(Class<U> configType, Function<ConfigType, T> getter, BiConsumer<ConfigType, T> setter,
                                               Function<T, U> parser, Function<U, T> unparser, BinaryOperator<T> merger) {
    this(configType, getter, setter, (config, loader) -> parser.apply(config), unparser, merger);
  }

  SimpleCoreServiceCreationConfigurationParser(Class<U> configType,
                                               Function<ConfigType, T> getter, BiConsumer<ConfigType, T> setter,
                                               Parser<T, U> parser, Function<U, T> unparser) {
    this(configType, getter, setter, parser, unparser, (a, b) -> { throw new IllegalStateException(); });
  }

  SimpleCoreServiceCreationConfigurationParser(Class<U> configType,
                                               Function<ConfigType, T> getter, BiConsumer<ConfigType, T> setter,
                                               Parser<T, U> parser, Function<U, T> unparser, BinaryOperator<T> merger) {
    this.configType = configType;
    this.getter = getter;
    this.setter = setter;
    this.parser = parser;
    this.unparser = unparser;
    this.merger = merger;
  }

  @Override
  public final FluentConfigurationBuilder<?> parseServiceCreationConfiguration(ConfigType root, ClassLoader classLoader, FluentConfigurationBuilder<?> builder) throws ClassNotFoundException {
    T config = getter.apply(root);
    if (config == null) {
      return builder;
    } else {
      return builder.withService(parser.parse(config, classLoader));
    }
  }

  @Override
  public ConfigType unparseServiceCreationConfiguration(Configuration configuration, ConfigType configType) {
    U config = ServiceUtils.findSingletonAmongst(this.configType, configuration.getServiceCreationConfigurations());
    if (config == null) {
      return configType;
    } else {
      T foo = getter.apply(configType);
      if (foo == null) {
        setter.accept(configType, unparser.apply(config));
      } else {
        setter.accept(configType, merger.apply(foo, unparser.apply(config)));
      }
      return configType;
    }
  }

  @FunctionalInterface
  interface Parser<T, U extends ServiceCreationConfiguration<?, ?>> {

    U parse(T t, ClassLoader classLoader) throws ClassNotFoundException;
  }
}
