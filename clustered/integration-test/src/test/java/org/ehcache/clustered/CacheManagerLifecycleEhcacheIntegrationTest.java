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
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.StateTransitionException;
import org.ehcache.xml.XmlConfiguration;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.entity.Entity;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManager;
import static org.junit.Assert.fail;

public class CacheManagerLifecycleEhcacheIntegrationTest {

  @ClassRule
  public static Cluster CLUSTER = new BasicExternalCluster(new File("build/cluster"), 1);

  @Test
  public void testAutoCreatedCacheManager() throws Exception {
    assertEntityNotExists(EhcacheClientEntity.class, "testAutoCreatedCacheManager");
    PersistentCacheManager manager = newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/testAutoCreatedCacheManager?auto-create")).build())
            .build();
    assertEntityNotExists(EhcacheClientEntity.class, "testAutoCreatedCacheManager");
    manager.init();
    try {
      assertEntityExists(EhcacheClientEntity.class, "testAutoCreatedCacheManager");
    } finally {
      manager.close();
    }

  }

  @Test
  public void testAutoCreatedCacheManagerUsingXml() throws Exception {
    URL xml = CacheManagerLifecycleEhcacheIntegrationTest.class.getResource("/configs/clustered.xml");
    URL substitutedXml = substitute(xml, "cluster-uri", CLUSTER.getConnectionURI().toString());
    PersistentCacheManager manager = (PersistentCacheManager) newCacheManager(new XmlConfiguration(substitutedXml));
    assertEntityNotExists(EhcacheClientEntity.class, "myCacheManager");
    manager.init();
    try {
      assertEntityExists(EhcacheClientEntity.class, "myCacheManager");
    } finally {
      manager.close();
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
    Connection connection = CLUSTER.newConnection();
    try {
      fetchEntity(connection, entityClazz, entityName);
    } catch (EntityNotFoundException ex) {
      throw new AssertionError(ex);
    } finally {
      connection.close();
    }
  }


  private static <T extends Entity> void assertEntityNotExists(Class<T> entityClazz, String entityName) throws ConnectionException, IOException {
    Connection connection = CLUSTER.newConnection();
    try {
      fetchEntity(connection, entityClazz, entityName);
      throw new AssertionError("Expected EntityNotFoundException");
    } catch (EntityNotFoundException ex) {
      //expected
    } finally {
      connection.close();
    }
  }

  private static <T extends Entity> void fetchEntity(Connection connection, Class<T> aClass, String myCacheManager) throws EntityNotFoundException, ConnectionException {
    try {
      connection.getEntityRef(aClass, 1, myCacheManager).fetchEntity().close();
    } catch (EntityNotProvidedException e) {
      throw new AssertionError(e);
    } catch (EntityVersionMismatchException e) {
      throw new AssertionError(e);
    }
  }

  private static URL substitute(URL input, String variable, String substitution) throws IOException {
    File output = File.createTempFile(input.getFile(), ".substituted", new File("build"));
    BufferedWriter writer = new BufferedWriter(new FileWriter(output));
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(input.openStream(), "UTF-8"));
      try {
        while (true) {
          String line = reader.readLine();
          if (line == null) {
            break;
          } else {
            writer.write(line.replace("${" + variable + "}", substitution));
            writer.newLine();
          }
        }
      } finally {
        reader.close();
      }
    } finally {
      writer.close();
    }
    return output.toURI().toURL();
  }
}
