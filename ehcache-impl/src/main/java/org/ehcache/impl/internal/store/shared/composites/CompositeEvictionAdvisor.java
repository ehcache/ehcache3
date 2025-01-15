/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.impl.internal.store.shared.composites;

import org.ehcache.config.EvictionAdvisor;

import java.util.Map;

public class CompositeEvictionAdvisor implements EvictionAdvisor<CompositeValue<?>, CompositeValue<?>> {
  private final Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap;

  public CompositeEvictionAdvisor(Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap) {
    this.evictionAdvisorMap = evictionAdvisorMap;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public boolean adviseAgainstEviction(CompositeValue<?> key, CompositeValue<?> value) {
    EvictionAdvisor<?, ?> evictionAdvisor = evictionAdvisorMap.get(key.getStoreId());
    if (evictionAdvisor == null) {
      return false;
    } else {
      return ((EvictionAdvisor) evictionAdvisor).adviseAgainstEviction(key.getValue(), value.getValue());
    }
  }
}
