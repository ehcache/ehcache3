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
package org.ehcache.config.thread;

import org.ehcache.config.Builder;
import org.ehcache.internal.executor.ContextAnalyzer;
import org.ehcache.internal.executor.ThreadFactoryProvider;
import org.ehcache.spi.service.ThreadPoolConfig;

public class EhcacheExecutorProviderConfigBuilder implements Builder<EhcacheExecutorProviderConfig> {

  private ThreadPoolConfig sharedCachedThreadPoolConfig;
  private int sharedScheduledThreadPoolCoreSize;
  private Class<? extends ThreadFactoryProvider> tfProvider;
  //private Class<? extends ContextAnalyzer> clazz;
  
  private EhcacheExecutorProviderConfigBuilder() {
  }
  
  private EhcacheExecutorProviderConfigBuilder(EhcacheExecutorProviderConfigBuilder other) {
    this.sharedCachedThreadPoolConfig = other.sharedCachedThreadPoolConfig;
    this.sharedScheduledThreadPoolCoreSize = other.sharedScheduledThreadPoolCoreSize;
    this.tfProvider = other.tfProvider;
//    this.clazz = other.clazz;
  }
  
  public static EhcacheExecutorProviderConfigBuilder newEhcacheExecutorProviderconfigBuilder() {
    return new EhcacheExecutorProviderConfigBuilder();
  }
  
  public EhcacheExecutorProviderConfigBuilder sharedCachedThreadPoolConfig(ThreadPoolConfig poolConfig) {
    EhcacheExecutorProviderConfigBuilder newBuilder = new EhcacheExecutorProviderConfigBuilder(this);
    newBuilder.sharedCachedThreadPoolConfig = poolConfig;
    return newBuilder;
  }
  
  public EhcacheExecutorProviderConfigBuilder sharedScheduledThreadPoolCoreSize(int coreSize) {
    EhcacheExecutorProviderConfigBuilder newBuilder = new EhcacheExecutorProviderConfigBuilder(this);
    newBuilder.sharedScheduledThreadPoolCoreSize = coreSize;
    return newBuilder;
  }

 /* public EhcacheExecutorProviderConfigBuilder contextAnalyzer(Class<? extends ContextAnalyzer> clazz) {
    EhcacheExecutorProviderConfigBuilder newBuilder = new EhcacheExecutorProviderConfigBuilder(this);
    newBuilder.clazz = clazz;
    return newBuilder;
  }*/
  
  public EhcacheExecutorProviderConfigBuilder threadFactoryProvider(Class<? extends ThreadFactoryProvider> tfProvider) {
    EhcacheExecutorProviderConfigBuilder newBuilder = new EhcacheExecutorProviderConfigBuilder(this);
    newBuilder.tfProvider = tfProvider;
    return newBuilder;
  } 
  
  @Override
  public EhcacheExecutorProviderConfig build() {
    DefaultEhcacheExecutorProviderConfig executorConfig = new DefaultEhcacheExecutorProviderConfig();
    executorConfig.setSharedCachedThreadPoolConfig(sharedCachedThreadPoolConfig);
    executorConfig.setSharedScheduledThreadPoolCoreSize(sharedScheduledThreadPoolCoreSize);
    //executorConfig.setContextAnalyzer(clazz);
    executorConfig.setThreadFactoryProvider(tfProvider);
    return executorConfig;
  }

}
