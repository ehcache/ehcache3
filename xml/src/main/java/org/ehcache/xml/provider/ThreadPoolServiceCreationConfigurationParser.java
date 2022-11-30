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

import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.ThreadPoolReferenceType;

import java.util.function.BiConsumer;
import java.util.function.Function;

class ThreadPoolServiceCreationConfigurationParser<T extends ServiceCreationConfiguration<?, ?>> extends SimpleCoreServiceCreationConfigurationParser<ThreadPoolReferenceType, T> {

  ThreadPoolServiceCreationConfigurationParser(Class<T> configType,
                                               Function<ConfigType, ThreadPoolReferenceType> getter, BiConsumer<ConfigType, ThreadPoolReferenceType> setter,
                                               Function<String, T> parser, Function<T, String> unparser) {
    super(configType, getter, setter, config -> parser.apply(config.getThreadPool()),
      config -> new ThreadPoolReferenceType().withThreadPool(unparser.apply(config)));
  }
}
