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

package org.ehcache.spi.service;

import org.ehcache.config.CacheConfiguration;

import java.io.File;

/**
 * @author Alex Snaps
 */
public interface LocalPersistenceService extends Service {

  Object persistenceContext(String cacheAlias, CacheConfiguration<?, ?> cacheConfiguration);

  @Deprecated
  File getDataFile(Object identifier);

  @Deprecated
  File getIndexFile(Object identifier);
}
