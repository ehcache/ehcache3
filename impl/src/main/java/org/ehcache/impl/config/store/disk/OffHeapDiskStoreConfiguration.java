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

package org.ehcache.impl.config.store.disk;

import org.ehcache.impl.internal.store.disk.OffHeapDiskStore;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 *
 * @author cdennis
 */
public class OffHeapDiskStoreConfiguration implements ServiceConfiguration<OffHeapDiskStore.Provider> {

  private final String threadPoolAlias;
  private final int writerConcurrency;

  public OffHeapDiskStoreConfiguration(String threadPoolAlias, int writerConcurrency) {
    this.threadPoolAlias = threadPoolAlias;
    this.writerConcurrency = writerConcurrency;
  }

  public String getThreadPoolAlias() {
    return threadPoolAlias;
  }

  public int getWriterConcurrency() {
    return writerConcurrency;
  }

  @Override
  public Class<OffHeapDiskStore.Provider> getServiceType() {
    return OffHeapDiskStore.Provider.class;
  }
}
