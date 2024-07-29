/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;

class SimpleCoreServiceCreationConfigurationParser<CONFIG, OUT, U extends ServiceCreationConfiguration<?, ?>> implements CoreServiceCreationConfigurationParser<CONFIG> {

  private final Class<U> configType;

  private final Function<CONFIG, OUT> getter;
  private final BiConsumer<CONFIG, OUT> setter;

  private final Parser<OUT, U> parser;
  private final Function<U, OUT> unparser;

  private final BinaryOperator<OUT> merger;

  SimpleCoreServiceCreationConfigurationParser(Class<U> configType,
                                               Function<CONFIG, OUT> getter, BiConsumer<CONFIG, OUT> setter,
                                               Function<OUT, U> parser, Function<U, OUT> unparser) {
    this(configType, getter, setter, (config, loader) -> parser.apply(config), unparser, (a, b) -> { throw new IllegalStateException(); });
  }

  SimpleCoreServiceCreationConfigurationParser(Class<U> configType,
                                               Function<CONFIG, OUT> getter, BiConsumer<CONFIG, OUT> setter,
                                               Parser<OUT, U> parser, Function<U, OUT> unparser) {
    this(configType, getter, setter, parser, unparser, (a, b) -> { throw new IllegalStateException(); });
  }

  SimpleCoreServiceCreationConfigurationParser(Class<U> configType,
                                               Function<CONFIG, OUT> getter, BiConsumer<CONFIG, OUT> setter,
                                               Parser<OUT, U> parser, Function<U, OUT> unparser, BinaryOperator<OUT> merger) {
    this.configType = configType;
    this.getter = getter;
    this.setter = setter;
    this.parser = parser;
    this.unparser = unparser;
    this.merger = merger;
  }

  @Override
  public final FluentConfigurationBuilder<?> parseServiceCreationConfiguration(CONFIG root, ClassLoader classLoader, FluentConfigurationBuilder<?> builder) throws ClassNotFoundException {
    OUT config = getter.apply(root);
    if (config == null) {
      return builder;
    } else {
      return builder.withService(parser.parse(config, classLoader));
    }
  }

  @Override
  public CONFIG unparseServiceCreationConfiguration(Configuration configuration, CONFIG configType) {
    U config = ServiceUtils.findSingletonAmongst(this.configType, configuration.getServiceCreationConfigurations());
    if (config == null) {
      return configType;
    } else {
      OUT foo = getter.apply(configType);
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
