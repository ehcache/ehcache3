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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.common.EhcacheEntityVersion;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.StateTransitionException;
import org.ehcache.xml.XmlConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.testing.rules.Cluster;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManager;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class CacheManagerLifecycleEhcacheIntegrationTest extends ClusteredTests {

  @ClassRule
  public static Cluster CLUSTER = newCluster().in(clusterPath())
    .withServiceFragment(offheapResource("primary-server-resource", 64)).build();
  private static Connection ASSERTION_CONNECTION;

  @BeforeClass
  public static void waitForActive() throws Exception {
    ASSERTION_CONNECTION = CLUSTER.newConnection();
  }

  @Test
  public void testAutoCreatedCacheManager() throws Exception {
    assertEntityNotExists(ClusterTierManagerClientEntity.class, "testAutoCreatedCacheManager");
    PersistentCacheManager manager = newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/testAutoCreatedCacheManager")).autoCreate(c -> c).build())
            .build();
    assertEntityNotExists(ClusterTierManagerClientEntity.class, "testAutoCreatedCacheManager");
    manager.init();
    try {
      assertEntityExists(ClusterTierManagerClientEntity.class, "testAutoCreatedCacheManager");
    } finally {
      manager.close();
    }

  }

  @Test
  public void testAutoCreatedCacheManagerUsingXml() throws Exception {
    URL xml = CacheManagerLifecycleEhcacheIntegrationTest.class.getResource("/configs/clustered.xml");
    URL substitutedXml = substitute(xml, "cluster-uri", CLUSTER.getConnectionURI().toString());
    PersistentCacheManager manager = (PersistentCacheManager) newCacheManager(new XmlConfiguration(substitutedXml));
    assertEntityNotExists(ClusterTierManagerClientEntity.class, "testAutoCreatedCacheManagerUsingXml");
    manager.init();
    try {
      assertEntityExists(ClusterTierManagerClientEntity.class, "testAutoCreatedCacheManagerUsingXml");
    } finally {
      manager.close();
    }
  }

  @Test
  public void testMultipleClientsAutoCreatingCacheManager() throws Exception {
    assertEntityNotExists(ClusterTierManagerClientEntity.class, "testMultipleClientsAutoCreatingCacheManager");

    final CacheManagerBuilder<PersistentCacheManager> managerBuilder = newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/testMultipleClientsAutoCreatingCacheManager")).autoCreate(c -> c).build());

    Callable<PersistentCacheManager> task = () -> {
      PersistentCacheManager manager = managerBuilder.build();
      manager.init();
      return manager;
    };

    assertEntityNotExists(ClusterTierManagerClientEntity.class, "testMultipleClientsAutoCreatingCacheManager");

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      List<Future<PersistentCacheManager>> results = executor.invokeAll(Collections.nCopies(4, task), 30, TimeUnit.SECONDS);
      for (Future<PersistentCacheManager> result : results) {
        assertThat(result.isDone(), is(true));
      }
      for (Future<PersistentCacheManager> result : results) {
        result.get().close();
      }
      assertEntityExists(ClusterTierManagerClientEntity.class, "testMultipleClientsAutoCreatingCacheManager");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testCacheManagerNotExistingFailsOnInit() throws Exception {
    try {
      newCacheManagerBuilder()
              .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/testCacheManagerNotExistingFailsOnInit")).build())
              .build(true);
      fail("Expected StateTransitionException");
    } catch (StateTransitionException e) {
      //expected
    }
  }

  private static <T extends Entity> void assertEntityExists(Class<T> entityClazz, String entityName) throws ConnectionException, IOException {
    try (Connection connection = getAssertionConnection()) {
      fetchEntity(connection, entityClazz, entityName);
    } catch (EntityNotFoundException ex) {
      throw new AssertionError(ex);
    }
  }


  private static <T extends Entity> void assertEntityNotExists(Class<T> entityClazz, String entityName) throws ConnectionException, IOException {
    try (Connection connection = getAssertionConnection()) {
      fetchEntity(connection, entityClazz, entityName);
      throw new AssertionError("Expected EntityNotFoundException");
    } catch (EntityNotFoundException ex) {
      //expected
    }
  }

  private static <T extends Entity> void fetchEntity(Connection connection, Class<T> aClass, String myCacheManager) throws EntityNotFoundException, ConnectionException {
    try {
      connection.getEntityRef(aClass, EhcacheEntityVersion.ENTITY_VERSION, myCacheManager).fetchEntity(null).close();
    } catch (EntityNotProvidedException | EntityVersionMismatchException e) {
      throw new AssertionError(e);
    }
  }

  private synchronized static Connection getAssertionConnection() throws ConnectionException {
    return new Connection() {
      @Override
      public<T extends Entity, C, U> EntityRef<T, C, U>  getEntityRef(Class<T> cls, long version, String name) throws EntityNotProvidedException {
        return ASSERTION_CONNECTION.getEntityRef(cls, version, name);
      }

      @Override
      public void close() throws IOException {
        //no-op
      }
    };
  }

  @AfterClass
  public static void closeAssertionConnection() throws IOException {
    ASSERTION_CONNECTION.close();
  }

  static URL substitute(URL input, String variable, String substitution) throws IOException {
    File output = File.createTempFile(input.getFile(), ".substituted", new File("build"));
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(output));
         BufferedReader reader = new BufferedReader(new InputStreamReader(input.openStream(), "UTF-8"))) {
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        } else {
          writer.write(line.replace("${" + variable + "}", substitution));
          writer.newLine();
        }
      }
    }
    return output.toURI().toURL();
  }
}
