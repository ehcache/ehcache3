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

package org.ehcache.clustered.writebehind;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class EvenNumberLoaderWriter<Long, String> implements CacheLoaderWriter<Long, String> {

  private final Map<Long, List<String>> records = new HashMap<>();

  @Override
  public synchronized String load(Long key) {
    List<String> list = records.get(key);
    return list == null ? null : list.get(list.size() - 1);
  }

  @Override
  public synchronized void write(Long key, String value) throws Exception {
    if (Integer.parseInt(key.toString()) % 2 != 0) {
      throw new RuntimeException("Only even keys can be inserted");
    }
    record(key, value);
  }

  @Override
  public void delete(Long key) {
  }

  private void record(Long key, String value) {
    records.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
  }

  synchronized Map<Long, List<String>> getRecords() {
    return Collections.unmodifiableMap(records);
  }
}
