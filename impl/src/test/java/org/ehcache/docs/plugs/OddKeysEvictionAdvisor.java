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
package org.ehcache.docs.plugs;

import org.ehcache.config.EvictionAdvisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ludovic Orban
 */
public class OddKeysEvictionAdvisor<K extends Number, V> implements EvictionAdvisor<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(OddKeysEvictionAdvisor.class);
  @Override
  public boolean adviseAgainstEviction(K key, V value) {
    boolean advice = (key.longValue() & 0x1) == 1;
    LOG.info("advising against eviction of {}? {}", key, advice);
    return advice;
  }
}
