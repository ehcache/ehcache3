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

package org.ehcache.clustered.client.config;

import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.ConnectionSource;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("deprecation")
public class ClusteringServiceConfigurationTest {

  private static final URI DEFAULT_URI = URI.create("terracotta://localhost:9450");
  private static final Iterable<InetSocketAddress> SERVERS = Collections.singletonList(InetSocketAddress.createUnresolved("localhost", 9450));
  private static final String CACHE_MANAGER = "cacheManager";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGetConnectionUrlNull() {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration((URI)null);
  }

  @Test
  public void testGetServersNull() {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(null, CACHE_MANAGER);
  }

  @Test
  public void testGetConnectionUrl() {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI).getClusterUri()).isEqualTo(DEFAULT_URI);
  }

  @Test
  public void testGetServersAndCacheManager() {
    ConnectionSource.ServerList connectionSource =  (ConnectionSource.ServerList) new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER).getConnectionSource();
    assertThat(connectionSource.getServers()).isEqualTo(SERVERS);
    assertThat(new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER).getConnectionSource().getClusterTierManager()).isEqualTo(CACHE_MANAGER);
  }

  @Test
  public void testGetServersAndRemove() {
    ConnectionSource.ServerList connectionSource =  (ConnectionSource.ServerList) new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER).getConnectionSource();
    Iterator<InetSocketAddress> iterator = connectionSource.getServers().iterator();
    iterator.next();
    iterator.remove();
    assertThat(connectionSource.getServers()).isEqualTo(SERVERS);
  }

  @Test
  public void testTimeoutsWithURI() {
    Timeouts timeouts = TimeoutsBuilder.timeouts().build();
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI, timeouts).getTimeouts()).isSameAs(timeouts);
  }

  @Test
  public void testTimeoutsWithServers() {
    Timeouts timeouts = TimeoutsBuilder.timeouts().build();
    assertThat(new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER, timeouts).getTimeouts()).isSameAs(timeouts);
  }

  @Test
  public void testDefaultTimeoutsWithURI() {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI).getTimeouts()).isEqualTo(TimeoutsBuilder.timeouts().build());
  }

  @Test
  public void testDefaultTimeoutsWithServers() {
    assertThat(new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER).getTimeouts()).isEqualTo(TimeoutsBuilder.timeouts().build());
  }

  @Test
  public void testTimeoutsCannotBeNull2ArgsWithURI() {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(DEFAULT_URI, (Timeouts) null);
  }

  @Test
  public void testTimeoutsCannotBeNull2ArgsWithServers() {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER, null);
  }

  @Test
  public void testTimeoutsCannotBeNull3ArgsWithURI() {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(DEFAULT_URI, null, new ServerSideConfiguration(Collections.emptyMap()));
  }

  @Test
  public void testTimeoutsCannotBeNull3ArgsWithServers() {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER, null, new ServerSideConfiguration(Collections.emptyMap()));
  }

  @Test
  public void testTimeoutsCannotBeNull4ArgsWithURI() {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(DEFAULT_URI, null, true, new ServerSideConfiguration(Collections.emptyMap()));
  }

  @Test
  public void testTimeoutsCannotBeNull4ArgsWithServers() {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER, null, true, new ServerSideConfiguration(Collections.emptyMap()));
  }

  @Test
  public void testGetServiceTypeWithURI() {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI).getServiceType()).isEqualTo(ClusteringService.class);
  }

  @Test
  public void testGetServiceTypeWithServers() {
    assertThat(new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER).getServiceType()).isEqualTo(ClusteringService.class);
  }

  @Test
  public void testGetAutoCreateWithURI() {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI, true,
        new ServerSideConfiguration(Collections.emptyMap())).isAutoCreate()).isTrue();
  }

  @Test
  public void testGetAutoCreateWithServers() {
    assertThat(new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER, true,
        new ServerSideConfiguration(Collections.emptyMap())).isAutoCreate()).isTrue();
  }

  @Test
  public void testBuilderWithURI() {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI)
        .builder(CacheManagerBuilder.newCacheManagerBuilder())).isExactlyInstanceOf(CacheManagerBuilder.class);
  }

  @Test
  public void testBuilderWithServers() {
    assertThat(new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER)
        .builder(CacheManagerBuilder.newCacheManagerBuilder())).isExactlyInstanceOf(CacheManagerBuilder.class);
  }

  @Test
  public void testReadableString() {
    ClusteringServiceConfiguration cfg;

    cfg = new ClusteringServiceConfiguration(SERVERS, CACHE_MANAGER);
    assertThat(cfg.readableString()).isNotNull();

    cfg = new ClusteringServiceConfiguration(DEFAULT_URI);
    assertThat(cfg.readableString()).isNotNull();

    cfg = new ClusteringServiceConfiguration(DEFAULT_URI, TimeoutsBuilder.timeouts().build());
    assertThat(cfg.readableString()).isNotNull();
  }

  @Test
  public void testDerivedConfiguration() {
    URI uri = URI.create("blah-blah");
    Timeouts timeouts = new Timeouts(ofSeconds(1), ofSeconds(2), ofSeconds(3));
    Map<String, ServerSideConfiguration.Pool> pools = singletonMap("default", new ServerSideConfiguration.Pool(42L, "resource"));
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfiguration("default", pools);
    Properties properties = new Properties();
    properties.setProperty("foo", "bar");

    ClusteringServiceConfiguration configuration = new ClusteringServiceConfiguration(uri, timeouts, true, serverSideConfiguration, properties);


    ClusteringServiceConfiguration derived = configuration.build(configuration.derive());

    assertThat(derived).isNotSameAs(configuration);
    assertThat(derived.getClusterUri()).isEqualTo(uri);
    assertThat(derived.getTimeouts()).isEqualTo(timeouts);
    assertThat(derived.getServerConfiguration().getDefaultServerResource()).isEqualTo("default");
    assertThat(derived.getServerConfiguration().getResourcePools()).isEqualTo(pools);
    assertThat(derived.getProperties()).isEqualTo(properties);
  }
}
