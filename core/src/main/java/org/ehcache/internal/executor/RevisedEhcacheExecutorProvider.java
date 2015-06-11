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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.ehcache.spi.service.Service;

/**
 * Different types of {@link ExecutorService} can be configured using service configuration builder.
 * 
 * Tasks can optionally provide an {@link TaskListener} to receive notifications of task status, through the use of {@link EhcacheManagedTask} interface.
 * <p>
 * Example:
 * <pre>
 * public class MyRunnable implements Runnable, EhcacheManagedTask {
 *   ...
 *   public void run() {
 *     ...
 *   }
 * 
 *   public TaskListener getTaskListener() {
 *     return null;
 *   }
 *   ...
 * }
 * </pre>
 * This facilitates task re-submission if it fails due to resource constraint.
 * </p>
 * 
 * 
 * 
 * @author palmanojkumar
 *
 */
public interface RevisedEhcacheExecutorProvider extends Service {

  /**
   * Returns managed instance of <em>shared</em> or <em>exclusive</em> {@link ExecutorService} based on {@link ExecutionContext}
   * <p>
   * The lifecycle of shared {@link ExecutorService} is managed by {@link RevisedEhcacheExecutorProvider} and 
   * invocation of such lifecycle methods by client will be ignored.
   * </p><p>
   * The lifecycle of exclusive {@link ExecutorService} is managed by {@link RevisedEhcacheExecutorProvider} but
   *  client can request to release it by invoking lifecycle methods.
   * </p>
   * 
   * @param config 
   * @param context used to decide type of {@link ExecutorService} to return
   * @return
   */
  ExecutorService getExecutorService(PoolConfig config,  ExecutionContext context);
  
  
  /**
   * Returns managed instance of <em>shared</em> or <em>exclusive</em> {@link ScheduledExecutorService} based on {@link ExecutionContext}
   * 
   * The lifecycle of shared {@link ScheduledExecutorService} is managed by {@link RevisedEhcacheExecutorProvider} and 
   * invocation of such lifecycle methods by client will throw exception {@link IllegalStateException}.
   * </p><p>
   * The lifecycle of exclusive {@link ScheduledExecutorService} is managed by {@link RevisedEhcacheExecutorProvider} but
   * client can request to release it by invoking its lifecycle methods.
   * 
   * @param context used to decide type of {@link ScheduledExecutorService} to return
   * @return 
   */
  ScheduledExecutorService getScheduledExecutorService(ExecutionContext context);
  
}
