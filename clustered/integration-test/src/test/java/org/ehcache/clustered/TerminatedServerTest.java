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

import com.google.code.tempusfugit.concurrency.ConcurrentTestRunner;

import org.assertj.core.api.ThrowableAssertAlternative;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.Diagnostics;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.impl.internal.statistics.DefaultStatisticsService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;
import org.terracotta.testing.rules.Cluster;

import com.tc.net.protocol.transport.ClientMessageTransport;
import com.tc.properties.TCProperties;
import com.tc.properties.TCPropertiesConsts;
import com.tc.properties.TCPropertiesImpl;

import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assume.assumeNoException;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

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
@RunWith(ConcurrentTestRunner.class)
public class TerminatedServerTest extends ClusteredTests {

  private static final int CLIENT_MAX_PENDING_REQUESTS = 5;

  /**
   * Determines the level of test concurrency.  The number of allowed concurrent tests
   * is set in {@link #setConcurrency()}.
   */
  private static final Semaphore TEST_PERMITS = new Semaphore(0);

  @ClassRule
  public static final TestCounter TEST_COUNTER = new TestCounter();

  @BeforeClass
  public static void setConcurrency() {
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    int testCount = TEST_COUNTER.getTestCount();
    /*
     * Some build environments can't reliably handle running tests in this class concurrently.
     * If the 'disable.concurrent.tests' system property is 'true', restrict the tests to
     * single operation using a single test permit.
     */
    boolean disableConcurrentTests = Boolean.getBoolean("disable.concurrent.tests");
    if (disableConcurrentTests) {
      TEST_PERMITS.release(1);
    } else {
      TEST_PERMITS.release(Math.min(Math.max(1, testCount / 2), availableProcessors));
    }
    System.out.format("TerminatedServerTest:" +
        " disableConcurrentTests=%b, testCount=%d, availableProcessors=%d, TEST_PERMITS.availablePermits()=%d%n",
        disableConcurrentTests, testCount, availableProcessors, TEST_PERMITS.availablePermits());
  }

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
        + "<ohr:offheap-resources>"
          + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
        + "</ohr:offheap-resources>"
   + "</config>"
   + "<service xmlns:lease='http://www.terracotta.org/service/lease'>"
     + "<lease:connection-leasing>"
       + "<lease:lease-length unit='seconds'>5</lease:lease-length>"
     + "</lease:connection-leasing>"
   + "</service>\n";

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

  private static Cluster createCluster() {
    try {
      return newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();
    } catch (IllegalArgumentException e) {
      assumeNoException(e);
      return null;
    }
  }

  @Rule
  public final TestName testName = new TestName();

  // Included in 'ruleChain' below.
  private final Cluster cluster = createCluster();


  // The TestRule.apply method is called on the inner-most Rule first with the result being passed to each
  // successively outer rule until the outer-most rule is reached. For ExternalResource rules, the before
  // method of each rule is called from outer-most rule to inner-most rule; the after method is called from
  // inner-most to outer-most.
  @Rule
  public final RuleChain ruleChain = RuleChain
      .outerRule(new TestConcurrencyLimiter())
      .around(cluster);

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
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    cluster.getClusterControl().terminateAllServers();

    new TimeLimitedTask<Void>(10, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    cluster.getClusterControl().terminateAllServers();

    cacheManager.close();

  }

  @Test
  public void testTerminationBeforeCacheManagerRetrieve() throws Exception {
    // Close all servers
    cluster.getClusterControl().terminateAllServers();

    // Try to retrieve an entity (that doesn't exist but I don't care... the server is not running anyway
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .timeouts(TimeoutsBuilder.timeouts().connection(Duration.ofSeconds(1))) // Need a connection timeout shorter than the TimeLimitedTask timeout
                    .expecting()
                    .defaultServerResource("primary-server-resource"));
    PersistentCacheManager cacheManagerExisting = clusteredCacheManagerBuilder.build(false);

    // Base test time limit on observed TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT; might not have been set in time to be effective
    long synackTimeout = TimeUnit.MILLISECONDS.toSeconds(ClientMessageTransport.TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT);

    assertExceptionOccurred(StateTransitionException.class,
      new TimeLimitedTask<Void>(3 + synackTimeout, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
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

    cluster.getClusterControl().terminateAllServers();

    assertExceptionOccurred(CachePersistenceException.class,
      new TimeLimitedTask<Void>(10, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    cluster.getClusterControl().terminateAllServers();

    assertExceptionOccurred(IllegalStateException.class,
      new TimeLimitedTask<Cache<Long, String>>(10, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    cluster.getClusterControl().terminateAllServers();

    cacheManager.removeCache("simple-cache");
  }

  @Test
  public void testTerminationThenGet() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                .timeouts(TimeoutsBuilder.timeouts().read(Duration.of(1, ChronoUnit.SECONDS)).build())
                .autoCreate()
                .defaultServerResource("primary-server-resource"))
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

    cluster.getClusterControl().terminateAllServers();

    String value = new TimeLimitedTask<String>(5, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                .timeouts(TimeoutsBuilder.timeouts().read(Duration.of(1, ChronoUnit.SECONDS)).build())
                .autoCreate()
                .defaultServerResource("primary-server-resource"))
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

    cluster.getClusterControl().terminateAllServers();

    boolean value = new TimeLimitedTask<Boolean>(5, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
              .timeouts(TimeoutsBuilder.timeouts().read(Duration.of(1, ChronoUnit.SECONDS)).build())
                .autoCreate()
                .defaultServerResource("primary-server-resource"))
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

    cluster.getClusterControl().terminateAllServers();

    Iterator<Cache.Entry<Long, String>> value = new TimeLimitedTask<Iterator<Cache.Entry<Long,String>>>(5, TimeUnit.SECONDS) {
      @Override
      Iterator<Cache.Entry<Long, String>> runTask() throws Exception {
        return cache.iterator();
      }
    }.run();

    assertThat(value.hasNext()).isFalse();
  }

  @Test
  public void testTerminationThenPut() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
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

    cluster.getClusterControl().terminateAllServers();

    // The resilience strategy will pick it up and not exception is thrown
    new TimeLimitedTask<Void>(10, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
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

    cluster.getClusterControl().terminateAllServers();

    // The resilience strategy will pick it up and not exception is thrown
    new TimeLimitedTask<String>(10, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
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

    cluster.getClusterControl().terminateAllServers();

    new TimeLimitedTask<Void>(10, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"))
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

    cluster.getClusterControl().terminateAllServers();

    // The resilience strategy will pick it up and not exception is thrown
    new TimeLimitedTask<Void>(10, TimeUnit.SECONDS) {
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
            .with(ClusteringServiceConfigurationBuilder.cluster(cluster.getConnectionURI().resolve("/MyCacheManagerName"))
              .timeouts(TimeoutsBuilder.timeouts()
                .read(readOperationTimeout))
              .autoCreate()
              .defaultServerResource("primary-server-resource"))
            .withCache("simple-cache",
              CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))))
            .build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
      cache.put(1L, "un");

      cluster.getClusterControl().terminateAllServers();

      // Fill the inflight queue and check that we wait no longer than the read timeout
      for (int i = 0; i < CLIENT_MAX_PENDING_REQUESTS; i++) {
        cache.get(1L);
      }

      // The resilience strategy will pick it up and not exception is thrown
      new TimeLimitedTask<Void>(readOperationTimeout.toMillis() * 2, TimeUnit.MILLISECONDS) { // I multiply by 2 to let some room after the expected timeout
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
   * Used as a {@link Rule @Rule} to limit the number of concurrently executing tests.
   */
  private final class TestConcurrencyLimiter extends ExternalResource {

    @Override
    protected void before() throws Throwable {
      try {
        TEST_PERMITS.acquire();
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
    }

    @Override
    protected void after() {
      TEST_PERMITS.release();
    }
  }

  /**
   * Used as a {@link org.junit.ClassRule @ClassRule} to determine the number of tests to
   * be run from the class.
   */
  private static final class TestCounter implements TestRule {

    private int testCount;

    @Override
    public Statement apply(Statement base, Description description) {
      int testCount = 0;
      for (Description child : description.getChildren()) {
        if (child.isTest()) {
          testCount++;
        }
      }
      this.testCount = testCount;

      return base;
    }

    private int getTestCount() {
      return testCount;
    }
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
        assertThat(isExpired).describedAs( "%s test thread exceeded its time limit of %d %s", testName.getMethodName(), timeLimit, unit).isFalse();
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
      FutureTask<Void> killer = new FutureTask<>(() -> {
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
      }, null);
      Thread killerThread = new Thread(killer, "Timeout Task - " + testName.getMethodName());
      killerThread.setDaemon(true);
      killerThread.start();

      return killer;
    }
  }
}
