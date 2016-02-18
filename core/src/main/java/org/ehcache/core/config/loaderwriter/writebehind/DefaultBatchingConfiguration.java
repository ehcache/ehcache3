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

package org.ehcache.core.config.loaderwriter.writebehind;

import java.util.concurrent.TimeUnit;

import org.ehcache.spi.loaderwriter.WriteBehindConfiguration.BatchingConfiguration;

/**
 *
 * @author cdennis
 */
public class DefaultBatchingConfiguration implements BatchingConfiguration {

  private final long maxDelay;
  private final TimeUnit maxDelayUnit;
  private final int batchSize;
  private final boolean coalescing;
  
  public DefaultBatchingConfiguration(long maxDelay, TimeUnit maxDelayUnit, int batchSize, boolean coalescing) {
    this.maxDelay = maxDelay;
    this.maxDelayUnit = maxDelayUnit;
    this.batchSize = batchSize;
    this.coalescing = coalescing;
  }

  @Override
  public long getMaxDelay() {
    return maxDelay;
  }

  @Override
  public TimeUnit getMaxDelayUnit() {
    return maxDelayUnit;
  }

  @Override
  public boolean isCoalescing() {
    return coalescing;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }
}
