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
package org.ehcache.config.writebehind;

import java.util.concurrent.TimeUnit;
import org.ehcache.config.Builder;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;

/**
 * @author Abhilash
 *
 */
public abstract class WriteBehindConfigurationBuilder implements Builder<WriteBehindConfiguration>  {
  
  protected Integer writeBehindConcurrency;
  protected Integer writeBehindMaxQueueSize;
  
  
  private WriteBehindConfigurationBuilder() {
  }

  private WriteBehindConfigurationBuilder(WriteBehindConfigurationBuilder other) {
    writeBehindConcurrency = other.writeBehindConcurrency;
    writeBehindMaxQueueSize = other.writeBehindMaxQueueSize;
  }

  public static BatchedWriteBehindConfigurationBuilder newBatchedWriteBehindConfiguration(long maxDelay, TimeUnit maxDelayUnit, int batchSize) {
    return new BatchedWriteBehindConfigurationBuilder(maxDelay, maxDelayUnit, batchSize);
  }
  
  public static UnBatchedWriteBehindConfigurationBuilder newUnBatchedWriteBehindConfiguration() {
    return new UnBatchedWriteBehindConfigurationBuilder();
  }

  public static class BatchedWriteBehindConfigurationBuilder extends WriteBehindConfigurationBuilder {
    private TimeUnit maxDelayUnit;
    private long maxDelay;
    private int batchSize;
    
    private Boolean coalescing;

    private BatchedWriteBehindConfigurationBuilder(long maxDelay, TimeUnit maxDelayUnit, int batchSize) {
      this.maxDelay = maxDelay;
      this.maxDelayUnit = maxDelayUnit;
      this.batchSize = batchSize;
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
      otherBuilder.batchSize = batchSize;
      return otherBuilder;
    }

    public BatchedWriteBehindConfigurationBuilder maxWriteDelay(long maxDelay, TimeUnit maxDelayUnit) {
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.maxDelay = maxDelay;
      otherBuilder.maxDelayUnit = maxDelayUnit;
      return otherBuilder;
    }

    @Override
    public BatchedWriteBehindConfigurationBuilder queueSize(int size) {
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.writeBehindMaxQueueSize = size;
      return otherBuilder;
    }

    @Override
    public BatchedWriteBehindConfigurationBuilder concurrencyLevel(int concurrency) {
      BatchedWriteBehindConfigurationBuilder otherBuilder = new BatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.writeBehindConcurrency = concurrency;
      return otherBuilder;
    }

    @Override
    public WriteBehindConfiguration build() {
      DefaultBatchingConfiguration configuration = new DefaultBatchingConfiguration(maxDelay, maxDelayUnit, batchSize);
      if (coalescing != null) {
        configuration.setCoalescing(coalescing);
      }
      return buildOn(new DefaultWriteBehindConfiguration(configuration));
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
      return buildOn(new DefaultWriteBehindConfiguration(null));
    }

    @Override
    public UnBatchedWriteBehindConfigurationBuilder queueSize(int size) {
      UnBatchedWriteBehindConfigurationBuilder otherBuilder = new UnBatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.writeBehindMaxQueueSize = size;
      return otherBuilder;
    }

    @Override
    public UnBatchedWriteBehindConfigurationBuilder concurrencyLevel(int concurrency) {
      UnBatchedWriteBehindConfigurationBuilder otherBuilder = new UnBatchedWriteBehindConfigurationBuilder(this);
      otherBuilder.writeBehindConcurrency = concurrency;
      return otherBuilder;
    }
  }

  public WriteBehindConfiguration buildOn(DefaultWriteBehindConfiguration configuration) {
    if (writeBehindConcurrency != null) {
      configuration.setConcurrency(writeBehindConcurrency);
    }
    if (writeBehindMaxQueueSize != null) {
      configuration.setMaxQueueSize(writeBehindMaxQueueSize);
    }
    return configuration;
  }
  
  public abstract WriteBehindConfigurationBuilder queueSize(int size);
  
  public abstract WriteBehindConfigurationBuilder concurrencyLevel(int concurrency);
}
