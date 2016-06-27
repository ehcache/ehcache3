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

package org.ehcache.clustered;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.Diagnostics;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.TestClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.store.StoreAccessTimeoutException;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.terracotta.connection.ConnectionException;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import com.tc.net.protocol.transport.ClientMessageTransport;
import com.tc.properties.TCProperties;
import com.tc.properties.TCPropertiesConsts;
import com.tc.properties.TCPropertiesImpl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Provides integration tests in which the server is terminated before the Ehcache operation completes.
 * <p>
 *   Tests in this class using the {@link TimeLimitedTask} class can be terminated by {@link Thread#interrupt()}
 *   and {@link Thread#stop()} (resulting in fielding a {@link ThreadDeath} exception).  Code in these tests
 *   <b>must not</b> intercept {@code ThreadDeath} and prevent thread termination.
 * </p>
 */
public class TerminatedServerTest {

  private static final String RESOURCE_CONFIG =
      "<service xmlns:ohr='http://www.terracotta.org/config/offheap-resource' id=\"resources\">"
          + "<ohr:offheap-resources>"
          + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
          + "</ohr:offheap-resources>" +
          "</service>\n";

  private static Map<String, String> OLD_PROPERTIES;

  @BeforeClass
  public static void setProperties() {
    Map<String, String> oldProperties = new HashMap<String, String>();

    /*
     * Control for a failed (timed out) connection attempt is not returned until
     * DistributedObjectClient.shutdownResources is complete.  This method attempts to shut down
     * support threads and is subject to a timeout of its own -- tc.properties
     * "l1.shutdown.threadgroup.gracetime" which has a default of 30 seconds -- and is co-dependent on
     * "tc.transport.handshake.timeout" with a default of 10 seconds.  The "tc.transport.handshake.timeout"
     * value is obtained during static initialization of com.tc.net.protocol.transport.ClientMessageTransport
     * -- the change here _may_ not be effective.
     */
    overrideProperty(oldProperties, TCPropertiesConsts.L1_SHUTDOWN_THREADGROUP_GRACETIME, "1000");
    overrideProperty(oldProperties, TCPropertiesConsts.TC_TRANSPORT_HANDSHAKE_TIMEOUT, "1000");

    OLD_PROPERTIES = oldProperties;
  }

  @AfterClass
  public static void restoreProperties() {
    if (OLD_PROPERTIES != null) {
      TCProperties tcProperties = TCPropertiesImpl.getProperties();
      for (Map.Entry<String, String> entry : OLD_PROPERTIES.entrySet()) {
        tcProperties.setProperty(entry.getKey(), entry.getValue());
      }
    }
  }

  @Rule
  public final TestName testName = new TestName();

  @Rule
  public final Cluster cluster =
      new BasicExternalCluster(new File("build/cluster"), 1, Collections.<File>emptyList(), "", RESOURCE_CONFIG, null);

  @Before
  public void waitForActive() throws Exception {
    cluster.getClusterControl().waitForActive();
  }

  /**
   * Tests if {@link CacheManager#close()} blocks if the client/server connection is disconnected.
   */
  @Test
  public void testTerminationBeforeCacheManagerClose() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
            .autoCreate()
            .defaultServerResource("primary-server-resource"));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    cluster.getClusterControl().tearDown();

    new TimeLimitedTask<Void>(2, TimeUnit.SECONDS) {
      @Override
      Void runTask() throws Exception {
        cacheManager.close();
        return null;
      }
    }.run();

    // TODO: Add assertion for successful CacheManager.init() following cluster restart (https://github.com/Terracotta-OSS/galvan/issues/30)
  }

  @Test
  public void testTerminationBeforeCacheManagerCloseWithCaches() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
                .lifecycleOperationTimeout(1, TimeUnit.SECONDS))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    cluster.getClusterControl().tearDown();

    new TimeLimitedTask<Void>(5, TimeUnit.SECONDS) {
      @Override
      Void runTask() throws Exception {
        cacheManager.close();
        return null;
      }
    }.run();
  }

  @Test
  public void testTerminationBeforeCacheManagerRetrieve() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
                .lifecycleOperationTimeout(1, TimeUnit.SECONDS));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
    cacheManager.close();

    cluster.getClusterControl().tearDown();

    clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .expecting()
                    .defaultServerResource("primary-server-resource"))
                .lifecycleOperationTimeout(1, TimeUnit.SECONDS));
    final PersistentCacheManager cacheManagerExisting = clusteredCacheManagerBuilder.build(false);

    // Base test time limit on observed TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT; might not have been set in time to be effective
    long synackTimeout = TimeUnit.MILLISECONDS.toSeconds(ClientMessageTransport.TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT);
    try {
      new TimeLimitedTask<Void>(2 + synackTimeout, TimeUnit.SECONDS) {
        @Override
        Void runTask() throws Exception {
          cacheManagerExisting.init();
          return null;
        }
      }.run();
      fail("Expecting StateTransitionException");
    } catch (StateTransitionException e) {
      assertThat(getCausalChain(e), hasItem(Matchers.<Throwable>instanceOf(ConnectionException.class)));
    }
  }

  @Ignore("Pending correction to https://github.com/ehcache/ehcache3/issues/1219")
  @Test
  public void testTerminationBeforeCacheManagerDestroyCache() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
                .lifecycleOperationTimeout(1, TimeUnit.SECONDS))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    cacheManager.removeCache("simple-cache");

    cluster.getClusterControl().tearDown();

    try {
      new TimeLimitedTask<Void>(5, TimeUnit.SECONDS) {
        @Override
        Void runTask() throws Exception {
          cacheManager.destroyCache("simple-cache");
          return null;
        }
      }.run();
      fail("Expecting CachePersistenceException");
    } catch (CachePersistenceException e) {
      assertThat(getUltimateCause(e), is(instanceOf(TimeoutException.class)));
    }
  }

  @Test
  public void testTerminationBeforeCacheCreate() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
                .lifecycleOperationTimeout(1, TimeUnit.SECONDS));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    cluster.getClusterControl().tearDown();

    try {
      new TimeLimitedTask<Cache<Long, String>>(5, TimeUnit.SECONDS) {
        @Override
        Cache<Long, String> runTask() throws Exception {
          return cacheManager.createCache("simple-cache",
              CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                  ResourcePoolsBuilder.newResourcePoolsBuilder()
                      .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
        }
      }.run();
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(getUltimateCause(e), is(instanceOf(TimeoutException.class)));
    }
  }

  @Test
  public void testTerminationBeforeCacheRemove() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
                .lifecycleOperationTimeout(1, TimeUnit.SECONDS))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    cluster.getClusterControl().tearDown();

    new TimeLimitedTask<Void>(5, TimeUnit.SECONDS) {
      @Override
      Void runTask() throws Exception {
        // CacheManager.removeCache silently "fails" when a timeout is recognized
        cacheManager.removeCache("simple-cache");
        return null;
      }
    }.run();
  }

  @Test
  public void testTerminationThenGet() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                .getOperationTimeout(1, TimeUnit.SECONDS)
                .autoCreate()
                .defaultServerResource("primary-server-resource"))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    assertThat(cache.get(2L), is(not(nullValue())));

    cluster.getClusterControl().tearDown();

    String value = new TimeLimitedTask<String>(5, TimeUnit.SECONDS) {
      @Override
      String runTask() throws Exception {
        return cache.get(2L);
      }
    }.run();

    assertThat(value, is(nullValue()));
  }

  @Test
  public void testTerminationThenContainsKey() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                .getOperationTimeout(1, TimeUnit.SECONDS)
                .autoCreate()
                .defaultServerResource("primary-server-resource"))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    assertThat(cache.containsKey(2L), is(true));

    cluster.getClusterControl().tearDown();

    boolean value = new TimeLimitedTask<Boolean>(5, TimeUnit.SECONDS) {
      @Override
      Boolean runTask() throws Exception {
        return cache.containsKey(2L);
      }
    }.run();

    assertThat(value, is(false));
  }

  @Ignore("ClusteredStore.iterator() is not implemented")
  @Test
  public void testTerminationThenIterator() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                .getOperationTimeout(1, TimeUnit.SECONDS)
                .autoCreate()
                .defaultServerResource("primary-server-resource"))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    cluster.getClusterControl().tearDown();

    Iterator<Cache.Entry<Long, String>> value = new TimeLimitedTask<Iterator<Cache.Entry<Long,String>>>(5, TimeUnit.SECONDS) {
      @Override
      Iterator<Cache.Entry<Long, String>> runTask() throws Exception {
        return cache.iterator();
      }
    }.run();

    assertThat(value.hasNext(), is(false));
  }

  @Test
  public void testTerminationThenPut() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
                .mutativeOperationTimeout(1, TimeUnit.SECONDS))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    cluster.getClusterControl().tearDown();

    try {
      new TimeLimitedTask<Void>(5, TimeUnit.SECONDS) {
        @Override
        Void runTask() throws Exception {
          cache.put(2L, "dos");
          return null;
        }
      }.run();
      fail("Expecting StoreAccessTimeoutException");
    } catch (StoreAccessTimeoutException e) {
      assertThat(e.getMessage(), containsString("Timeout exceeded for SERVER_STORE_OP/GET_AND_APPEND"));
    }
  }

  @Test
  public void testTerminationThenPutIfAbsent() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
                .mutativeOperationTimeout(1, TimeUnit.SECONDS))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    cluster.getClusterControl().tearDown();

    try {
      new TimeLimitedTask<String>(5, TimeUnit.SECONDS) {
        @Override
        String runTask() throws Exception {
          return cache.putIfAbsent(2L, "dos");
        }
      }.run();
      fail("Expecting StoreAccessTimeoutException");
    } catch (StoreAccessTimeoutException e) {
      assertThat(e.getMessage(), containsString("Timeout exceeded for SERVER_STORE_OP/GET_AND_APPEND"));
    }
  }

  @Test
  public void testTerminationThenRemove() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
                .mutativeOperationTimeout(1, TimeUnit.SECONDS))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    cluster.getClusterControl().tearDown();

    try {
      new TimeLimitedTask<Void>(5, TimeUnit.SECONDS) {
        @Override
        Void runTask() throws Exception {
          cache.remove(2L);
          return null;
        }
      }.run();
      fail("Expecting StoreAccessTimeoutException");
    } catch (StoreAccessTimeoutException e) {
      assertThat(e.getMessage(), containsString("Timeout exceeded for SERVER_STORE_OP/GET_AND_APPEND"));
    }
  }

  @Test
  public void testTerminationThenClear() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(TestClusteringServiceConfiguration.of(
                ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
                .mutativeOperationTimeout(1, TimeUnit.SECONDS))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    cluster.getClusterControl().tearDown();

    try {
      new TimeLimitedTask<Void>(5, TimeUnit.SECONDS) {
        @Override
        Void runTask() throws Exception {
          cache.clear();
          return null;
        }
      }.run();
      fail("Expecting StoreAccessTimeoutException");
    } catch (StoreAccessTimeoutException e) {
      assertThat(e.getMessage(), containsString("Timeout exceeded for SERVER_STORE_OP/CLEAR"));
    }
  }

  private Throwable getUltimateCause(Throwable t) {
    Throwable ultimateCause = t;
    while (ultimateCause.getCause() != null) {
      ultimateCause = ultimateCause.getCause();
    }
    return ultimateCause;
  }

  private List<Throwable> getCausalChain(Throwable t) {
    ArrayList<Throwable> causalChain = new ArrayList<Throwable>();
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      causalChain.add(cause);
    }
    return causalChain;
  }

  private static void overrideProperty(Map<String, String> oldProperties, String propertyName, String propertyValue) {
    TCProperties tcProperties = TCPropertiesImpl.getProperties();
    oldProperties.put(propertyName, tcProperties.getProperty(propertyName));
    tcProperties.setProperty(propertyName, propertyValue);
  }

  /**
   * Runs a method under control of a timeout.
   *
   * @param <V> the return type of the method
   */
  @SuppressWarnings("deprecation")
  private abstract class TimeLimitedTask<V> {

    /**
     * Synchronization lock used to prevent split between recognition of test expiration & thread interruption
     * and test task completion & thread interrupt clear.
     */
    private final byte[] lock = new byte[0];
    private final long timeLimit;
    private final TimeUnit unit;
    private volatile boolean isDone = false;
    private volatile boolean isExpired = false;

    private TimeLimitedTask(long timeLimit, TimeUnit unit) {
      this.timeLimit = timeLimit;
      this.unit = unit;
    }

    /**
     * The time-limited task to run.
     *
     * @return a possibly {@code null} result of type {@code <V>}
     * @throws Exception if necessary
     */
    abstract V runTask() throws Exception;

    /**
     * Invokes {@link #runTask()} under a time limit.  If {@code runTask} execution exceeds the amount of
     * time specified in the {@link TimeLimitedTask#TimeLimitedTask(long, TimeUnit) constructor}, the task
     * {@code Thread} is first interrupted and, if the thread remains alive for another duration of the time
     * limit, the thread is forcefully stopped using {@link Thread#stop()}.
     *
     * @return the result from {@link #runTask()}
     *
     * @throws ThreadDeath if {@code runTask} is terminated by {@code Thread.stop}
     * @throws AssertionError if {@code runTask} did not complete before the timeout
     * @throws Exception if thrown by {@code runTask}
     */
    V run() throws Exception {

      V result;
      Future<Void> future = interruptAfter(timeLimit, unit);
      try {
        result = this.runTask();
      } finally {
        synchronized (lock) {
          isDone = true;
          future.cancel(true);
          Thread.interrupted();     // Reset interrupted status
        }
        assertThat(testName.getMethodName() + " test thread exceeded its time limit of " + timeLimit + " " + unit, isExpired, is(false));
      }

      return result;
    }

    /**
     * Starts a {@code Thread} to terminate the calling thread after a specified interval.
     * If the timeout expires, a thread dump is taken and the current thread interrupted.
     *
     * @param interval the amount of time to wait
     * @param unit the unit for {@code interval}
     *
     * @return a {@code Future} that may be used to cancel the timeout.
     */
    private Future<Void> interruptAfter(final long interval, final TimeUnit unit) {
      final Thread targetThread = Thread.currentThread();
      FutureTask<Void> killer = new FutureTask<Void>(new Runnable() {
        @Override
        public void run() {
          try {
            unit.sleep(interval);
            if (!isDone && targetThread.isAlive()) {
              synchronized (lock) {
                if (isDone) {
                  return;       // Let test win completion race
                }
                isExpired = true;
                System.out.format("%n%n%s test is stalled; taking a thread dump and terminating the test%n%n",
                    testName.getMethodName());
                Diagnostics.threadDump(System.out);
                targetThread.interrupt();
              }

            /*                NEVER DO THIS AT HOME!
             * This code block uses a BAD, BAD, BAD, BAD deprecated method to ensure the target thread
             * is terminated.  This is done to prevent a test stall from methods using a "non-interruptible"
             * looping wait where the interrupt status is recorded but ignored until the awaited event
             * occurs.
             */
              unit.timedJoin(targetThread, interval);
              if (!isDone && targetThread.isAlive()) {
                System.out.format("%s test thread did not respond to Thread.interrupt; forcefully stopping %s%n",
                    testName.getMethodName(), targetThread);
                targetThread.stop();   // Deprecated - BAD CODE!
              }
            }
          } catch (InterruptedException e) {
            // Interrupted when canceled; simple exit
          }
        }
      }, null);
      Thread killerThread = new Thread(killer, "Timeout Task - " + testName.getMethodName());
      killerThread.setDaemon(true);
      killerThread.start();

      return killer;
    }
  }
}
