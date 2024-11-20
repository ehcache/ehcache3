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

package org.ehcache.xml;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheType;
import org.w3c.dom.Document;

public interface CoreServiceConfigurationParser {

  <K, V> CacheConfigurationBuilder<K, V> parseServiceConfiguration(CacheTemplate cacheDefinition, ClassLoader cacheClassLoader,
                                                                   CacheConfigurationBuilder<K, V> cacheBuilder) throws ClassNotFoundException;

  CacheType unparseServiceConfiguration(CacheConfiguration<?, ?> cacheConfiguration, CacheType cacheType);
}
