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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.PoolDefinition;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ClusteringServiceConfigurationTest {

  @Test(expected = NullPointerException.class)
  public void testGetConnectionUrlNull() throws Exception {
    new ClusteringServiceConfiguration(null, Collections.<String, PoolDefinition>emptyMap());
  }

  @Test
  public void testGetConnectionUrl() throws Exception {
    final URI connectionUrl = URI.create("http://localhost:9450");
    assertThat(new ClusteringServiceConfiguration(connectionUrl, Collections.<String, PoolDefinition>emptyMap()).getClusterUri(), is(connectionUrl));
  }

  @Test
  public void testGetServiceType() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("http://localhost:9450"), Collections.<String, PoolDefinition>emptyMap()).getServiceType(),
        is(equalTo(ClusteringService.class)));
  }

  @Test
  public void testBuilder() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("http://localhost:9450"), Collections.<String, PoolDefinition>emptyMap())
        .builder(CacheManagerBuilder.newCacheManagerBuilder()), is(instanceOf(CacheManagerBuilder.class)));
  }
}