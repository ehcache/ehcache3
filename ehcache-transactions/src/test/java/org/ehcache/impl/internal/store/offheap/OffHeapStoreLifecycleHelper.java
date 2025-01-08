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

package org.ehcache.impl.internal.store.offheap;

/*
 * This is an import of a shaded class because we depend on the shaded distribution jar.
 * This means we have to use the shaded StatisticsManager when digging in to the internals like this.
 */
import org.ehcache.shadow.org.terracotta.statistics.StatisticsManager;

/**
 * @author Ludovic Orban
 */
public class OffHeapStoreLifecycleHelper {

  private OffHeapStoreLifecycleHelper() {
  }

  public static void init(OffHeapStore<?, ?> offHeapStore) {
    OffHeapStore.Provider.init(offHeapStore);
  }

  public static void close(OffHeapStore<?, ?> offHeapStore) {
    OffHeapStore.Provider.close(offHeapStore);
    StatisticsManager.nodeFor(offHeapStore).clean();
  }

}
