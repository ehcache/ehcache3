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

package org.ehcache.config.builders;

import java.util.concurrent.TimeUnit;

import org.ehcache.core.config.loaderwriter.writebehind.DefaultBatchingConfiguration;
import org.ehcache.core.config.loaderwriter.writebehind.DefaultWriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration.BatchingConfiguration;

/**
 * @author Abhilash
 *
 */
public abstract class WriteBehindConfigurationBuilder implements Builder<WriteBehindConfiguration>  {
  
  protected int concurrency = 1;
  protected int queueSize = Integer.MAX_VALUE;
  protected String threadPoolAlias = null;
  
  private WriteBehindConfigurationBuilder() {
  }

  private WriteBehindConfigurationBuilder(WriteBehindConfigurationBuilder other) {
    concurrency = other.concurrency;
    queueSize = other.queueSize;
    threadPoolAlias = other.threadPoolAlias;
  }

  public static BatchedWriteBehindConfigurationBuilder newBatchedWriteBehindConfiguration(long maxDelay, TimeUnit maxDelayUnit, int batchSize) {
    return new BatchedWriteBehindConfigurationBuilder(maxDelay, maxDelayUnit, batchSize);
  }
  
  public static UnBatchedWriteBehindConfigurationBuilder newUnBatchedWriteBehindConfiguration() {
    return new UnBatchedWriteBehindConfigurationBuilder();
  }

  public static final class BatchedWriteBehindConfigurationBuilder extends WriteBehindConfigurationBuilder {
    private TimeUnit maxDelayUnit;
    private long maxDelay;
    private int batchSize;
    private boolean coalescing = false;
    
    private BatchedWriteBehindConfigurationBuilder(long maxDelay, TimeUnit maxDelayUnit, int batchSize) {
      setMaxWriteDelay(maxDelay, maxDelayUnit);
      setBatchSize(batchSize);
    }

    private BatchedWriteBehindConfigurationBuilder(BatchedWriteBehindConfigurationBuilder other) {
      super(other);
      maxDelay = other.maxDelay;
      maxDelayUnit = other.maxDelayUnit;
      coalescing = other.coalescing;
      batchSize = other.batchSize;
    }

    public BatchedWriteBehindConfigurationBuilder enableCoalescing() {
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.coalescing = true;
      return otherBuilder;
    }

    public BatchedWriteBehindConfigurationBuilder disableCoalescing() {
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.coalescing = false;
      return otherBuilder;
    }

    public BatchedWriteBehindConfigurationBuilder batchSize(int batchSize) {
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.setBatchSize(batchSize);
      return otherBuilder;
    }

    private void setBatchSize(int batchSize) throws IllegalArgumentException {
      if (batchSize < 1) {
        throw new IllegalArgumentException("Batch size must be a positive integer, was: " + batchSize);
      }
      this.batchSize = batchSize;
    }

    public BatchedWriteBehindConfigurationBuilder maxWriteDelay(long maxDelay, TimeUnit maxDelayUnit) {
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.setMaxWriteDelay(maxDelay, maxDelayUnit);
      return otherBuilder;
    }

    private void setMaxWriteDelay(long maxDelay, TimeUnit maxDelayUnit) throws IllegalArgumentException {
      if (maxDelay < 1) {
        throw new IllegalArgumentException("Max batch delay must be positive, was: " + maxDelay + " " + maxDelayUnit);
      }
      this.maxDelay = maxDelay;
      this.maxDelayUnit = maxDelayUnit;
    }

    @Override
    public BatchedWriteBehindConfigurationBuilder queueSize(int size) {
      if (size < 1) {
        throw new IllegalArgumentException("Queue size must be positive, was: " + size);
      }
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.queueSize = size;
      return otherBuilder;
    }

    @Override
    public BatchedWriteBehindConfigurationBuilder concurrencyLevel(int concurrency) {
      if (concurrency < 1) {
        throw new IllegalArgumentException("Concurrency must be positive, was: " + concurrency);
      }
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.concurrency = concurrency;
      return otherBuilder;
    }
    
    @Override
    public BatchedWriteBehindConfigurationBuilder useThreadPool(String alias) {
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.threadPoolAlias = alias;
      return otherBuilder;
    }

    @Override
    public WriteBehindConfiguration build() {
      return buildWith(new DefaultBatchingConfiguration(maxDelay, maxDelayUnit, batchSize, coalescing));
    }
  }
  
  public static class UnBatchedWriteBehindConfigurationBuilder extends WriteBehindConfigurationBuilder {

    private UnBatchedWriteBehindConfigurationBuilder() {
    }
    
    private UnBatchedWriteBehindConfigurationBuilder(UnBatchedWriteBehindConfigurationBuilder other) {
      super(other);
    }

    @Override
    public WriteBehindConfiguration build() {
      return buildWith(null);
    }

    @Override
    public UnBatchedWriteBehindConfigurationBuilder queueSize(int size) {
      if (size < 1) {
        throw new IllegalArgumentException("Queue size must be positive, was: " + size);
      }
      UnBatchedWriteBehindConfigurationBuilder otherBuilder = new UnBatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.queueSize = size;
      return otherBuilder;
    }

    @Override
    public UnBatchedWriteBehindConfigurationBuilder concurrencyLevel(int concurrency) {
      if (concurrency < 1) {
        throw new IllegalArgumentException("Concurrency must be positive, was: " + concurrency);
      }
      UnBatchedWriteBehindConfigurationBuilder otherBuilder = new UnBatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.concurrency = concurrency;
      return otherBuilder;
    }
    
    @Override
    public UnBatchedWriteBehindConfigurationBuilder useThreadPool(String alias) {
      UnBatchedWriteBehindConfigurationBuilder otherBuilder = new UnBatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.threadPoolAlias = alias;
      return otherBuilder;
    }
  }

  public WriteBehindConfiguration buildWith(BatchingConfiguration batching) {
    return new DefaultWriteBehindConfiguration(threadPoolAlias, concurrency, queueSize, batching);
  }
  
  public abstract WriteBehindConfigurationBuilder queueSize(int size);
  
  public abstract WriteBehindConfigurationBuilder concurrencyLevel(int concurrency);

  public abstract WriteBehindConfigurationBuilder useThreadPool(String alias);
}
