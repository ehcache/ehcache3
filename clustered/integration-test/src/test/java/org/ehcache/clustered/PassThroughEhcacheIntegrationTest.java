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

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.Entity;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.passthrough.PassthroughServer;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManager;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.junit.Assert.fail;

public class PassThroughEhcacheIntegrationTest {

  @Test
  public void testAutoCreatedCacheManager() throws Exception {
    UnitTestConnectionService.reset();
    assertEntityNotExists(EhcacheClientEntity.class, "myCacheManager");
    PersistentCacheManager manager = newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://example.com:9540/myCacheManager?auto-create")).build())
            .build();
    assertEntityNotExists(EhcacheClientEntity.class, "myCacheManager");
    manager.init();
    try {
      assertEntityExists(EhcacheClientEntity.class, "myCacheManager");
    } finally {
      manager.close();
    }

  }

  @Test
  public void testAutoCreatedCacheManagerUsingXml() throws Exception {
    UnitTestConnectionService.reset();
    URL xml = PassThroughEhcacheIntegrationTest.class.getResource("/configs/clustered.xml");
    PersistentCacheManager manager = (PersistentCacheManager) newCacheManager(new XmlConfiguration(xml));
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
    UnitTestConnectionService.reset();
    try {
      newCacheManagerBuilder()
              .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("http://example.com:9540/myCacheManager")).build())
              .build(true);
      fail("Expected StateTransitionException");
    } catch (StateTransitionException e) {
      //expected
    }
  }

  private static <T extends Entity> void assertEntityExists(Class<T> entityClazz, String entityName) {
    try {
      fetchEntity(UnitTestConnectionService.server(), entityClazz, entityName);
    } catch (EntityNotFoundException ex) {
      throw new AssertionError(ex);
    }
  }

  private static <T extends Entity> void assertEntityNotExists(Class<T> entityClazz, String entityName) {
    try {
      fetchEntity(UnitTestConnectionService.server(), entityClazz, entityName);
      throw new AssertionError("Expected EntityNotFoundException");
    } catch (EntityNotFoundException ex) {
      //expected
    }
  }

  private static <T extends Entity> void fetchEntity(PassthroughServer server, Class<T> aClass, String myCacheManager) throws EntityNotFoundException {
    Connection connection = server.connectNewClient();
    try {
      try {
        connection.getEntityRef(aClass, 1, myCacheManager).fetchEntity().close();
      } catch (EntityNotProvidedException e) {
        throw new AssertionError(e);
      } catch (EntityVersionMismatchException e) {
        throw new AssertionError(e);
      }
    } finally {
      try {
        connection.close();
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    }
  }
}
