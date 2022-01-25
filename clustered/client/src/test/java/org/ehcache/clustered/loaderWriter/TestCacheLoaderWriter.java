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

package org.ehcache.clustered.loaderWriter;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestCacheLoaderWriter implements CacheLoaderWriter<Long, String> {

  public final Map<Long, String> storeMap = new ConcurrentHashMap<>();

  @Override
  public String load(Long key) throws Exception {
    return storeMap.get(key);
  }

  @Override
  public void write(Long key, String value) throws Exception {
    storeMap.put(key, value);
  }

  @Override
  public void delete(Long key) throws Exception {
    storeMap.remove(key);
  }
}
