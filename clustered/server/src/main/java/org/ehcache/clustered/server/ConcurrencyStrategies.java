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
package org.ehcache.clustered.server;

import java.util.Set;
import org.terracotta.entity.ConcurrencyStrategy;

import static java.util.Collections.singleton;
import org.terracotta.entity.EntityMessage;

public final class ConcurrencyStrategies {

  private static final ConcurrencyStrategy NO_CONCURRENCY = new ConcurrencyStrategy<EntityMessage>() {
    @Override
    public int concurrencyKey(EntityMessage message) {
      return 0;
    }

    @Override
    public Set<Integer> getKeysForSynchronization() {
      return singleton(0);
    }
  };

  private ConcurrencyStrategies() {

  }

  public static final <T extends EntityMessage> ConcurrencyStrategy<T> noConcurrency() {
    return NO_CONCURRENCY;
  }
}
