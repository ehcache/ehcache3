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

import org.assertj.core.api.ThrowableAssertAlternative;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.util.ParallelTestCluster;
import org.ehcache.clustered.util.runners.Parallel;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.terracotta.utilities.test.Diagnostics;

import com.tc.net.protocol.transport.ClientMessageTransport;
import com.tc.properties.TCProperties;
import com.tc.properties.TCPropertiesConsts;
import com.tc.properties.TCPropertiesImpl;
import org.terracotta.utilities.test.rules.TestRetryer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;
import static org.terracotta.utilities.test.rules.TestRetryer.OutputIs.CLASS_RULE;
import static org.terracotta.utilities.test.rules.TestRetryer.OutputIs.RULE;
import static org.terracotta.utilities.test.rules.TestRetryer.tryValues;

/**
 * Provides integration tests in which the server is terminated before the Ehcache operation completes.
 * <p>
 * Tests in this class using the {@link TimeLimitedTask} class can be terminated by {@link Thread#interrupt()}
 * and {@link Thread#stop()} (resulting in fielding a {@link ThreadDeath} exception).  Code in these tests
 * <b>must not</b> intercept {@code ThreadDeath} and prevent thread termination.
 */
// =============================================================================================
// The tests in this class are run **in parallel** to avoid long run times caused by starting
// and stopping a server for each test.  Each test and the environment supporting it must have
// no side effects which can affect another test.
// =============================================================================================
@RunWith(Parallel.class)
public class TerminatedServerTest extends ClusteredTests {

  private static final int CLIENT_MAX_PENDING_REQUESTS = 5;

  private static Map<String, String> OLD_PROPERTIES;

  @BeforeClass
  public static void setProperties() {
    Map<String, String> oldProperties = new HashMap<>();

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

    // Used only by testTerminationFreezesTheClient to be able to fill the inflight queue
    overrideProperty(oldProperties, TCPropertiesConsts.CLIENT_MAX_PENDING_REQUESTS, Integer.toString(CLIENT_MAX_PENDING_REQUESTS));

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

  private <T extends Throwable> ThrowableAssertAlternative<T> assertExceptionOccurred(Class<T> exception, TimeLimitedTask<?> task) {
    return assertThatExceptionOfType(exception)
      .isThrownBy(() -> task.run());
  }

  @ClassRule @Rule
  public static final TestRetryer<Duration, ParallelTestCluster> CLUSTER = tryValues(ofSeconds(2), ofSeconds(10), ofSeconds(30))
    .map(leaseLength -> new ParallelTestCluster(
      newCluster().in(clusterPath()).withServiceFragment(
        offheapResource("primary-server-resource", 64) + leaseLength(leaseLength)).build()))
    .outputIs(CLASS_RULE, RULE);

  @Rule
  public final TestName testName = new TestName();

  @Before
  public void startAllServers() throws Exception {
    CLUSTER.get().getClusterControl().startAllServers();
  }

  /**
   * Tests if {@link CacheManager#close()} blocks if the client/server connection is disconnected.
   */
  @Test
  public void testTerminationBeforeCacheManagerClose() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
            .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    CLUSTER.get().getClusterControl().terminateAllServers();

    new TimeLimitedTask<Void>(CLUSTER.input().plusSeconds(10)) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                    .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    CLUSTER.get().getClusterControl().terminateAllServers();

    cacheManager.close();

  }

  @Test
  public void testTerminationBeforeCacheManagerRetrieve() throws Exception {
    // Close all servers
    CLUSTER.get().getClusterControl().terminateAllServers();

    // Try to retrieve an entity (that doesn't exist but I don't care... the server is not running anyway
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                    .timeouts(TimeoutsBuilder.timeouts().connection(Duration.ofSeconds(1))) // Need a connection timeout shorter than the TimeLimitedTask timeout
                    .expecting(server -> server.defaultServerResource("primary-server-resource")));
    PersistentCacheManager cacheManagerExisting = clusteredCacheManagerBuilder.build(false);

    // Base test time limit on observed TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT; might not have been set in time to be effective
    long synackTimeout = TimeUnit.MILLISECONDS.toSeconds(ClientMessageTransport.TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT);

    assertExceptionOccurred(StateTransitionException.class,
      new TimeLimitedTask<Void>(ofSeconds(3 + synackTimeout)) {
        @Override
        Void runTask() {
          cacheManagerExisting.init();
          return null;
        }
      })
      .withRootCauseInstanceOf(TimeoutException.class);
  }

  @Test
  @Ignore("Works but by sending a really low level exception. Need to be fixed to get the expected CachePersistenceException")
  public void testTerminationBeforeCacheManagerDestroyCache() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                    .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    cacheManager.removeCache("simple-cache");

    CLUSTER.get().getClusterControl().terminateAllServers();

    assertExceptionOccurred(CachePersistenceException.class,
      new TimeLimitedTask<Void>(ofSeconds(10)) {
        @Override
        Void runTask() throws Exception {
          cacheManager.destroyCache("simple-cache");
          return null;
        }
      });
  }

  @Test
  @Ignore("There are no timeout on the create cache right now. It waits until the server comes back")
  public void testTerminationBeforeCacheCreate() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                    .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    CLUSTER.get().getClusterControl().terminateAllServers();

    assertExceptionOccurred(IllegalStateException.class,
      new TimeLimitedTask<Cache<Long, String>>(ofSeconds(10)) {
        @Override
        Cache<Long, String> runTask() throws Exception {
          return cacheManager.createCache("simple-cache",
              CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                  ResourcePoolsBuilder.newResourcePoolsBuilder()
                      .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
        }
      })
      .withRootCauseInstanceOf(TimeoutException.class);
  }

  @Test
  public void testTerminationBeforeCacheRemove() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                    .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    CLUSTER.get().getClusterControl().terminateAllServers();

    cacheManager.removeCache("simple-cache");
  }

  @Test
  public void testTerminationThenGet() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                .timeouts(TimeoutsBuilder.timeouts().read(Duration.of(1, ChronoUnit.SECONDS)).build())
                .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    assertThat(cache.get(2L)).isNotNull();

    CLUSTER.get().getClusterControl().terminateAllServers();

    String value = new TimeLimitedTask<String>(ofSeconds(5)) {
      @Override
      String runTask() throws Exception {
        return cache.get(2L);
      }
    }.run();

    assertThat(value).isNull();
  }

  @Test
  public void testTerminationThenContainsKey() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                .timeouts(TimeoutsBuilder.timeouts().read(Duration.of(1, ChronoUnit.SECONDS)).build())
                .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    assertThat(cache.containsKey(2L)).isTrue();

    CLUSTER.get().getClusterControl().terminateAllServers();

    boolean value = new TimeLimitedTask<Boolean>(ofSeconds(5)) {
      @Override
      Boolean runTask() throws Exception {
        return cache.containsKey(2L);
      }
    }.run();

    assertThat(value).isFalse();
  }

  @Ignore("ClusteredStore.iterator() is not implemented")
  @Test
  public void testTerminationThenIterator() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
              .timeouts(TimeoutsBuilder.timeouts().read(Duration.of(1, ChronoUnit.SECONDS)).build())
                .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    CLUSTER.get().getClusterControl().terminateAllServers();

    Iterator<Cache.Entry<Long, String>> value = new TimeLimitedTask<Iterator<Cache.Entry<Long,String>>>(ofSeconds(5)) {
      @Override
      Iterator<Cache.Entry<Long, String>> runTask() {
        return cache.iterator();
      }
    }.run();

    assertThat(value.hasNext()).isFalse();
  }

  @Test
  public void testTerminationThenPut() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    CLUSTER.get().getClusterControl().terminateAllServers();

    // The resilience strategy will pick it up and not exception is thrown
    new TimeLimitedTask<Void>(ofSeconds(10)) {
      @Override
      Void runTask() throws Exception {
        cache.put(2L, "dos");
        return null;
      }
    }.run();
  }

  @Test
  public void testTerminationThenPutIfAbsent() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    CLUSTER.get().getClusterControl().terminateAllServers();

    // The resilience strategy will pick it up and not exception is thrown
    new TimeLimitedTask<String>(ofSeconds(10)) {
      @Override
      String runTask() throws Exception {
        return cache.putIfAbsent(2L, "dos");
      }
    }.run();
  }

  @Test
  public void testTerminationThenRemove() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    CLUSTER.get().getClusterControl().terminateAllServers();

    new TimeLimitedTask<Void>(ofSeconds(10)) {
      @Override
      Void runTask() throws Exception {
        cache.remove(2L);
        return null;
      }
    }.run();
  }

  @Test
  public void testTerminationThenClear() throws Exception {
    StatisticsService statisticsService = new DefaultStatisticsService();
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .using(statisticsService)
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    CLUSTER.get().getClusterControl().terminateAllServers();

    // The resilience strategy will pick it up and not exception is thrown
    new TimeLimitedTask<Void>(ofSeconds(10)) {
        @Override
        Void runTask() {
          cache.clear();
          return null;
        }
      }.run();
  }

  /**
   * If the server goes down, the client should not freeze on a server call. It should timeout and answer using
   * the resilience strategy. Whatever the number of calls is done afterwards.
   *
   * @throws Exception
   */
  @Test
  public void testTerminationFreezesTheClient() throws Exception {
    Duration readOperationTimeout = Duration.ofMillis(100);

    try(PersistentCacheManager cacheManager =
          CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.get().getConnectionURI().resolve("/").resolve(testName.getMethodName()))
              .timeouts(TimeoutsBuilder.timeouts()
                .read(readOperationTimeout))
              .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
            .withCache("simple-cache",
              CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))))
            .build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
      cache.put(1L, "un");

      CLUSTER.get().getClusterControl().terminateAllServers();

      // Fill the inflight queue and check that we wait no longer than the read timeout
      for (int i = 0; i < CLIENT_MAX_PENDING_REQUESTS; i++) {
        cache.get(1L);
      }

      // The resilience strategy will pick it up and not exception is thrown
      new TimeLimitedTask<Void>(readOperationTimeout.multipliedBy(2)) { // I multiply by 2 to let some room after the expected timeout
        @Override
        Void runTask() {
          cache.get(1L); // the call that could block
          return null;
        }
      }.run();

    } catch(StateTransitionException e) {
      // On the cacheManager.close(), it waits for the lease to expire and then throw this exception
    }
  }

  private static void overrideProperty(Map<String, String> oldProperties, String propertyName, String propertyValue) {
    TCProperties tcProperties = TCPropertiesImpl.getProperties();
    oldProperties.put(propertyName, tcProperties.getProperty(propertyName, true));
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
    private final Duration timeLimit;
    private volatile boolean isDone = false;
    private volatile boolean isExpired = false;

    private TimeLimitedTask(Duration timeLimit) {
      this.timeLimit = timeLimit;
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
     * time specified in the {@link TimeLimitedTask#TimeLimitedTask(Duration) constructor}, the task
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
      Future<Void> future = interruptAfter(timeLimit);
      try {
        result = this.runTask();
      } finally {
        synchronized (lock) {
          isDone = true;
          future.cancel(true);
          Thread.interrupted();     // Reset interrupted status
        }
        assertThat(isExpired).describedAs( "%s test thread exceeded its time limit of %s", testName.getMethodName(), timeLimit).isFalse();
      }

      return result;
    }

    /**
     * Starts a {@code Thread} to terminate the calling thread after a specified interval.
     * If the timeout expires, a thread dump is taken and the current thread interrupted.
     *
     * @param interval the amount of time to wait
     *
     * @return a {@code Future} that may be used to cancel the timeout.
     */
    private Future<Void> interruptAfter(Duration interval) {
      final Thread targetThread = Thread.currentThread();
      FutureTask<Void> killer = new FutureTask<>(() -> {
        try {
          Thread.sleep(interval.toMillis());
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
            targetThread.join(interval.toMillis());
            if (!isDone && targetThread.isAlive()) {
              System.out.format("%s test thread did not respond to Thread.interrupt; forcefully stopping %s%n",
                testName.getMethodName(), targetThread);
              targetThread.stop();   // Deprecated - BAD CODE!
            }
          }
        } catch (InterruptedException e) {
          // Interrupted when canceled; simple exit
        }
      }, null);
      Thread killerThread = new Thread(killer, "Timeout Task - " + testName.getMethodName());
      killerThread.setDaemon(true);
      killerThread.start();

      return killer;
    }
  }
}
