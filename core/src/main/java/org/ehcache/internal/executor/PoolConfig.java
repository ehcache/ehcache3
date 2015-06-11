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
package org.ehcache.internal.executor;

import java.util.concurrent.TimeUnit;

import org.ehcache.spi.service.ExecutorServiceType;

/**
 * @author palmanojkumar
 *
 */
public class PoolConfig {

  private int corePoolSize;
  private int maximumThreads;
  private int keepAliveTime;
  private TimeUnit timeUnit;

  private ExecutorServiceType executorType;

  //constructors...
  
  public int getCorePoolSize() {
    return corePoolSize;
  }

  public void setCorePoolSize(int corePoolSize) {
    this.corePoolSize = corePoolSize;
  }

  public int getMaximumThreads() {
    return maximumThreads;
  }

  public void setMaximumThreads(int maximumThreads) {
    this.maximumThreads = maximumThreads;
  }

  public int getKeepAliveTime() {
    return keepAliveTime;
  }

  public void setKeepAliveTime(int keepAliveTime) {
    this.keepAliveTime = keepAliveTime;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
  }

  public ExecutorServiceType getExecutorType() {
    return executorType;
  }

  public void setExecutorType(ExecutorServiceType executorType) {
    this.executorType = executorType;
  }
  
  
  
}
