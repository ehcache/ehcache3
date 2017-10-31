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

import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static net.bytebuddy.matcher.ElementMatchers.is;
import static org.assertj.core.api.Assertions.assertThat;

public class ClusteringServiceConfigurationTest {

  private static URI DEFAULT_URI = URI.create("terracotta://localhost:9450");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGetConnectionUrlNull() throws Exception {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration((URI)null);
  }

  @Test
  public void testGetConnectionUrl() throws Exception {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI).getClusterUri()).isEqualTo(DEFAULT_URI);
  }

  @Test
  public void testOperationTimeouts() throws Exception {
    Timeouts timeouts = Timeouts.builder().build();
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI, timeouts).getOperationTimeouts()).isSameAs(timeouts);
  }

  @Test
  public void testDefaultOperationTimeouts() throws Exception {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI).getOperationTimeouts()).isEqualTo(Timeouts.builder().build());
  }

  @Test
  public void testOperationTimeoutsCannotBeNull2Args() throws Exception {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(DEFAULT_URI, (Timeouts) null);
  }

  @Test
  public void testOperationTimeoutsCannotBeNull3Args() throws Exception {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(DEFAULT_URI, (Timeouts) null, new ServerSideConfiguration(Collections.emptyMap()));
  }

  @Test
  public void testOperationTimeoutsCannotBeNull4Args() throws Exception {
    expectedException.expect(NullPointerException.class);
    new ClusteringServiceConfiguration(DEFAULT_URI, (Timeouts) null, true, new ServerSideConfiguration(Collections.emptyMap()));
  }

  @Test
  public void testGetServiceType() throws Exception {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI).getServiceType()).isEqualTo(ClusteringService.class);
  }

  @Test
  public void testGetAutoCreate() throws Exception {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI, true,
        new ServerSideConfiguration(Collections.emptyMap())).isAutoCreate()).isTrue();
  }

  @Test
  public void testBuilder() throws Exception {
    assertThat(new ClusteringServiceConfiguration(DEFAULT_URI)
        .builder(CacheManagerBuilder.newCacheManagerBuilder())).isExactlyInstanceOf(CacheManagerBuilder.class);
  }
}
