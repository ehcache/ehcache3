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
package org.ehcache.clustered.management;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.management.entity.nms.NmsConfig;
import org.terracotta.management.entity.nms.client.DefaultNmsService;
import org.terracotta.management.entity.nms.client.IllegalManagementCallException;
import org.terracotta.management.entity.nms.client.NmsEntity;
import org.terracotta.management.entity.nms.client.NmsEntityFactory;
import org.terracotta.management.entity.nms.client.NmsService;
import org.terracotta.management.model.cluster.ServerEntity;
import org.terracotta.management.model.cluster.ServerEntityIdentifier;
import org.terracotta.testing.rules.Cluster;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;

public final class ClusterWithManagement implements TestRule {

  private final Cluster cluster;

  private Connection managementConnection;
  private NmsService nmsService;
  private ServerEntityIdentifier tmsServerEntityIdentifier;

  public ClusterWithManagement(Cluster cluster) {
    this.cluster = cluster;
  }

  protected void before() throws Throwable {
    this.managementConnection = cluster.newConnection();
    this.nmsService = createNmsService(managementConnection);
    while ((tmsServerEntityIdentifier = nmsService.readTopology()
      .activeServerEntityStream()
      .filter(serverEntity -> serverEntity.getType().equals(NmsConfig.ENTITY_TYPE))
      .filter(ServerEntity::isManageable)
      .map(ServerEntity::getServerEntityIdentifier)
      .findFirst()
      .orElse(null)) == null) {
      sleep(100);
    }
    startCollectingServerEntityStats();
  }

  protected void after() throws Exception {
    try {
      nmsService.readTopology().getSingleStripe().getActiveServerEntity(tmsServerEntityIdentifier)
        .ifPresent(client -> {
          try {
            nmsService.stopStatisticCollector(client.getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    } finally {
      managementConnection.close();
    }
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return cluster.apply(new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before();
        try {
          base.evaluate();
          after();
        } catch (Throwable t) {
          try {
            after();
            throw t;
          } catch (Exception e) {
            throw new MultipleFailureException(asList(t, e));
          }
        }
      }
    }, description);
  }

  public Cluster getCluster() {
    return cluster;
  }

  public NmsService getNmsService() {
    return nmsService;
  }

  public ServerEntityIdentifier getTmsServerEntityIdentifier() {
    return tmsServerEntityIdentifier;
  }

  private static NmsService createNmsService(Connection connection) throws ConnectionException, EntityConfigurationException {
    NmsEntityFactory entityFactory = new NmsEntityFactory(connection, AbstractClusteringManagementTest.class.getName());
    NmsEntity tmsAgentEntity = entityFactory.retrieveOrCreate(new NmsConfig());

    NmsService nmsService = new DefaultNmsService(tmsAgentEntity);
    nmsService.setOperationTimeout(10, TimeUnit.SECONDS);
    return nmsService;
  }

  public void startCollectingServerEntityStats() throws InterruptedException, ExecutionException, TimeoutException, IllegalManagementCallException {
    ServerEntity manageableEntity = nmsService.readTopology().getSingleStripe().getActiveServerEntity(tmsServerEntityIdentifier).filter(ServerEntity::isManageable).get();
    nmsService.startStatisticCollector(manageableEntity.getContext(), 1, TimeUnit.SECONDS).waitForReturn();
  }
}
