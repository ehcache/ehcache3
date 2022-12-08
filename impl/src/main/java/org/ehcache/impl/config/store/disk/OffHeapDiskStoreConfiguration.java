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
 * {@link ServiceConfiguration} for the default {@link org.ehcache.core.spi.store.Store off heap disk store}.
 */
public class OffHeapDiskStoreConfiguration implements ServiceConfiguration<OffHeapDiskStore.Provider, OffHeapDiskStoreConfiguration> {

  public static final int DEFAULT_WRITER_CONCURRENCY = 1;
  public static final int DEFAULT_DISK_SEGMENTS = 16;

  private final String threadPoolAlias;
  private final int writerConcurrency;
  private final int diskSegments;

  /**
   * Creates a new configuration instance using the provided {@code diskSegments}. Other attributes are set to their default
   * ({@code null} for {@code threadPoolAlias} and {@link #DEFAULT_WRITER_CONCURRENCY} for {@code writerConcurrency}).
   *
   * @param diskSegments number of disk segments allocated. The more disk segments there is, the more concurrency you get but
   *               the more resources you are using (mainly file pointers)
   */
  public OffHeapDiskStoreConfiguration(int diskSegments) {
    this(null, DEFAULT_WRITER_CONCURRENCY, diskSegments);
  }

  /**
   * Creates a new configuration instance using the provided parameters. {@code diskSegments} is set to {@link #DEFAULT_DISK_SEGMENTS}.
   *
   * @param threadPoolAlias the thread pool alias
   * @param writerConcurrency the writer concurrency
   *
   * @see org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration
   */
  public OffHeapDiskStoreConfiguration(String threadPoolAlias, int writerConcurrency) {
    this(threadPoolAlias, writerConcurrency, DEFAULT_DISK_SEGMENTS);
  }

  /**
   * Creates a new configuration instance using the provided parameters.
   *
   * @param threadPoolAlias the thread pool alias
   * @param writerConcurrency the writer concurrency
   * @param diskSegments number of disk segments allocated. The more disk segments there is, the more concurrency you get but
   *               the more resources you are using (mainly file pointers)
   *
   * @see org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration
   */
  public OffHeapDiskStoreConfiguration(String threadPoolAlias, int writerConcurrency, int diskSegments) {
    this.threadPoolAlias = threadPoolAlias;
    this.writerConcurrency = writerConcurrency;
    this.diskSegments = diskSegments;
  }

  /**
   * Returns the configured thread pool alias.
   *
   * @return the thread pool alias
   *
   * @see org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration
   */
  public String getThreadPoolAlias() {
    return threadPoolAlias;
  }

  /**
   * Returns the configured writer concurrency
   *
   * @return the writer concurrency
   */
  public int getWriterConcurrency() {
    return writerConcurrency;
  }

  /**
   * Returns the number of disk segments created
   *
   * @return the number of segments
   */
  public int getDiskSegments() {
    return diskSegments;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<OffHeapDiskStore.Provider> getServiceType() {
    return OffHeapDiskStore.Provider.class;
  }

  @Override
  public OffHeapDiskStoreConfiguration derive() {
    return new OffHeapDiskStoreConfiguration(threadPoolAlias, writerConcurrency, diskSegments);
  }

  @Override
  public OffHeapDiskStoreConfiguration build(OffHeapDiskStoreConfiguration config) {
    return config;
  }
}
