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
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class ClusteringServiceConfigurationTest {

  @Test(expected = IllegalArgumentException.class)
  public void testGetConnectionUrlNull() throws Exception {
    new ClusteringServiceConfiguration(null, false, null, Collections.<String, PoolDefinition>emptyMap());
  }

  @Test
  public void testGetConnectionUrl() throws Exception {
    final URI connectionUrl = URI.create("terracotta://localhost:9450");
    assertThat(new ClusteringServiceConfiguration(connectionUrl, false, null, Collections.<String, PoolDefinition>emptyMap()).getClusterUri(), is(connectionUrl));
  }

  @Test
  public void testGetServiceType() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("terracotta://localhost:9450"), false, null, Collections.<String, PoolDefinition>emptyMap()).getServiceType(),
        is(equalTo(ClusteringService.class)));
  }

  @Test
  public void testGetAutoCreate() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("terracotta://localhost:9450"), true, null, Collections.<String, PoolDefinition>emptyMap()).isAutoCreate(),
        is(true));
  }

  @Test
  public void testCtorPoolsWithDefault() throws Exception {
    Map<String, PoolDefinition> poolDefinitionMap = new HashMap<String, PoolDefinition>();
    poolDefinitionMap.put("sharedPool", new PoolDefinition(8L, MemoryUnit.MB));
    ClusteringServiceConfiguration configuration = new ClusteringServiceConfiguration(
        URI.create("terracotta://localhost:9450"), false, "defaultResource", poolDefinitionMap);
    Map<String, PoolDefinition> actualPools = configuration.getPools();
    assertThat(actualPools.size(), is(1));
    Map.Entry<String, PoolDefinition> actualPool = actualPools.entrySet().iterator().next();
    assertThat(actualPool.getKey(), is("sharedPool"));
    assertThat(actualPool.getValue().getServerResource(), is(nullValue()));     // resource set in DefaultClusteringService.extractResourcePools()
    assertThat(actualPool.getValue().getSize(), is(8L));
    assertThat(actualPool.getValue().getUnit(), is(MemoryUnit.MB));
  }

  @Test
  public void testCtorPoolsWithoutDefault() throws Exception {
    Map<String, PoolDefinition> poolDefinitionMap = new HashMap<String, PoolDefinition>();
    poolDefinitionMap.put("sharedPool", new PoolDefinition(8L, MemoryUnit.MB));
    try {
      new ClusteringServiceConfiguration(URI.create("terracotta://localhost:9450"), false, null, poolDefinitionMap);
      fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString(" no default value "));
    }
  }

  @Test
  public void testBuilder() throws Exception {
    assertThat(new ClusteringServiceConfiguration(URI.create("terracotta://localhost:9450"), false, null, Collections.<String, PoolDefinition>emptyMap())
        .builder(CacheManagerBuilder.newCacheManagerBuilder()), is(instanceOf(CacheManagerBuilder.class)));
  }

  @Test
  public void testInvalidURI() {

    URI uri = URI.create("http://localhost:9540");
    try {
      new ClusteringServiceConfiguration(uri, true, "default", null);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Cluster Uri is not valid, clusterUri : http://localhost:9540"));
    }
  }

  @Test
  public void testValidURI() {
    URI uri = URI.create("terracotta://localhost:9540");
    ClusteringServiceConfiguration serviceConfiguration = new ClusteringServiceConfiguration(uri, true, "default", null);;

    assertThat(serviceConfiguration.getClusterUri(), is(uri));
  }
}