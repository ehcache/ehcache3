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

import org.ehcache.internal.executor.ContextAnalyzer;
import org.ehcache.internal.executor.RevisedEhcacheExecutorProvider;
import org.ehcache.internal.executor.ThreadFactoryProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ThreadPoolConfig;

public interface EhcacheExecutorProviderConfig extends ServiceConfiguration<RevisedEhcacheExecutorProvider> {

  ThreadPoolConfig getSharedCachedThreadPoolConfig();

  int getSharedScheduledThreadPoolCoreSize();

  Class<? extends ThreadFactoryProvider> getThreadFactoryProvider();
  
  //Class<? extends ContextAnalyzer> getContextAnalyzer();
}