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

package org.ehcache.xml.service;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.xml.CoreServiceConfigurationParser;
import org.ehcache.xml.model.CacheTemplate;

import java.util.function.Function;

class SimpleCoreServiceConfigurationParser<T> implements CoreServiceConfigurationParser {

  private final Function<CacheTemplate, T> extractor;
  private final Parser<T> parser;

  SimpleCoreServiceConfigurationParser(Function<CacheTemplate, T> extractor, Function<T, ServiceConfiguration<?>> parser) {
    this(extractor, (config, loader) -> parser.apply(config));
  }

  SimpleCoreServiceConfigurationParser(Function<CacheTemplate, T> extractor, Parser<T> parser) {
    this.extractor = extractor;
    this.parser = parser;
  }

  @Override
  public final <K, V> CacheConfigurationBuilder<K, V> parseServiceConfiguration(CacheTemplate cacheDefinition, ClassLoader cacheClassLoader, CacheConfigurationBuilder<K, V> cacheBuilder) throws ClassNotFoundException {
    T config = extractor.apply(cacheDefinition);
    if (config == null) {
      return cacheBuilder;
    } else {
      return cacheBuilder.add(parser.parse(config, cacheClassLoader));
    }
  }

  @FunctionalInterface
  interface Parser<T> {

    ServiceConfiguration<?> parse(T t, ClassLoader classLoader) throws ClassNotFoundException;
  }
}
