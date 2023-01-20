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
package org.ehcache.impl.internal.store.heap;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.expiry.Expiry;
import org.ehcache.core.spi.time.TimeSource;

public abstract class OnHeapStoreByRefTest extends BaseOnHeapStoreTest {

  @Override
  protected <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super K, ? super V> expiry,
      final EvictionAdvisor<? super K, ? super V> evictionAdvisor) {
    return newStore(timeSource, expiry, evictionAdvisor, 100);
  }

  protected abstract <K, V> OnHeapStore<K, V> newStore(TimeSource timeSource,
      Expiry<? super K, ? super V> expiry,
      EvictionAdvisor<? super K, ? super V> evictionAdvisor, int capacity);

}
