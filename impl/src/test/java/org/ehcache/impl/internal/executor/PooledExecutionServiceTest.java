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
package org.ehcache.impl.internal.executor;

import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban
 */
public class PooledExecutionServiceTest {

  @Test
  public void testEmptyConfigThrowsAtStart() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    PooledExecutionService pooledExecutionService = new PooledExecutionService(configuration);

    try {
      pooledExecutionService.start(null);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testGetOrderedExecutorFailsOnNonExistentPool() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    configuration.addPool("aaa", 0, 1);
    PooledExecutionService pooledExecutionService = new PooledExecutionService(configuration);

    pooledExecutionService.start(null);

    try {
      pooledExecutionService.getOrderedExecutor("abc", new LinkedBlockingDeque<Runnable>());
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    pooledExecutionService.stop();
  }

  @Test
  public void testGetOrderedExecutorFailsOnNonExistentDefaultPool() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    configuration.addPool("aaa", 0, 1);
    PooledExecutionService pooledExecutionService = new PooledExecutionService(configuration);

    pooledExecutionService.start(null);

    try {
      pooledExecutionService.getOrderedExecutor(null, new LinkedBlockingDeque<Runnable>());
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    pooledExecutionService.stop();
  }

  @Test
  public void testGetOrderedExecutorSucceedsOnExistingPool() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    configuration.addPool("aaa", 0, 1);
    PooledExecutionService pooledExecutionService = new PooledExecutionService(configuration);

    pooledExecutionService.start(null);

    ExecutorService aaa = pooledExecutionService.getOrderedExecutor("aaa", new LinkedBlockingDeque<Runnable>());
    aaa.shutdown();

    pooledExecutionService.stop();
  }

  @Test
  public void testGetOrderedExecutorSucceedsOnExistingDefaultPool() throws Exception {
    PooledExecutionServiceConfiguration configuration = new PooledExecutionServiceConfiguration();
    configuration.addDefaultPool("dflt", 0, 1);
    PooledExecutionService pooledExecutionService = new PooledExecutionService(configuration);

    pooledExecutionService.start(null);

    ExecutorService dflt = pooledExecutionService.getOrderedExecutor(null, new LinkedBlockingDeque<Runnable>());
    dflt.shutdown();

    pooledExecutionService.stop();
  }

}
