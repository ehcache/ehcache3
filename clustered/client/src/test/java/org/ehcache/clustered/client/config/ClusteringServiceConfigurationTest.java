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
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ClusteringServiceConfigurationTest {

  @Test(expected = NullPointerException.class)
  public void testGetConnectionUrlNull() throws Exception {
    new ClusteringServiceConfiguration((URI)null);
  }

  @Test
  public void testGetConnectionUrl() throws Exception {
    final URI connectionUrl = URI.create("terracotta://localhost:9450");
    assertThat(new ClusteringServiceConfiguration(connectionUrl).getClusterUri(), is(connectionUrl));
  }

  @Test
  public void testGetGetTimeout() throws Exception {
    final URI connectionUrl = URI.create("terracotta://localhost:9450");
    final TimeoutDuration getTimeout = TimeoutDuration.of(15, TimeUnit.SECONDS);
    assertThat(new ClusteringServiceConfiguration(connectionUrl, getTimeout).getReadOperationTimeout(), is(getTimeout));

    assertThat(new ClusteringServiceConfiguration(connectionUrl).getReadOperationTimeout(), is(nullValue()));
    assertThat(new ClusteringServiceConfiguration(connectionUrl, null).getReadOperationTimeout(), is(nullValue()));
  }

  @Test
  public void testGetServiceType() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("terracotta://localhost:9450")).getServiceType(),
        is(equalTo(ClusteringService.class)));
  }

  @Test
  public void testGetAutoCreate() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("terracotta://localhost:9450"), true,
            new ServerSideConfiguration(Collections.<String, Pool>emptyMap())).isAutoCreate(),
        is(true));
  }

  @Test
  public void testBuilder() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("terracotta://localhost:9450"))
        .builder(CacheManagerBuilder.newCacheManagerBuilder()), is(instanceOf(CacheManagerBuilder.class)));
  }

  @Test
  public void testValidURI() {
    URI uri = URI.create("terracotta://localhost:9540");
    ClusteringServiceConfiguration serviceConfiguration = new ClusteringServiceConfiguration(uri);

    assertThat(serviceConfiguration.getClusterUri(), is(uri));
  }
}