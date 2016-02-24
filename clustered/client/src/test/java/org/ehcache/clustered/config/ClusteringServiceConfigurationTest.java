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

package org.ehcache.clustered.config;

import org.ehcache.clustered.ClusteredCacheManagerBuilder;
import org.ehcache.clustered.service.ClusteringService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
public class ClusteringServiceConfigurationTest {

  @Test(expected = NullPointerException.class)
  public void testGetConnectionUrlNull() throws Exception {
    new ClusteringServiceConfiguration(null);
  }

  @Test
  public void testGetConnectionUrl() throws Exception {
    final URI connectionUrl = URI.create("http://localhost:9450");
    assertThat(new ClusteringServiceConfiguration(connectionUrl).getConnectionUrl(), is(connectionUrl));
  }

  @Test
  public void testGetServiceType() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("http://localhost:9450")).getServiceType(),
        is(equalTo(ClusteringService.class)));
  }

  @Test
  public void testBuilder() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("http://localhost:9450"))
        .builder(CacheManagerBuilder.newCacheManagerBuilder()), is(instanceOf(ClusteredCacheManagerBuilder.class)));
  }
}