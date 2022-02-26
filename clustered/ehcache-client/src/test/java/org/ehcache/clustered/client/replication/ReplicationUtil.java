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

package org.ehcache.clustered.client.replication;

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.impl.serialization.CompactJavaSerializer;

import java.lang.reflect.Field;

import static org.ehcache.config.units.MemoryUnit.MB;

public class ReplicationUtil {

  private ReplicationUtil() {

  }

  public static ClusterTierClientEntity getEntity(ServerStoreProxy clusteringService) throws NoSuchFieldException, IllegalAccessException {
    Field entity = clusteringService.getClass().getDeclaredField("entity");
    entity.setAccessible(true);
    return (ClusterTierClientEntity)entity.get(clusteringService);
  }

  public static ClusterTierManagerClientEntity getEntity(ClusteringService clusteringService) throws NoSuchFieldException, IllegalAccessException {
    Field entity = clusteringService.getClass().getDeclaredField("entity");
    entity.setAccessible(true);
    return (ClusterTierManagerClientEntity)entity.get(clusteringService);
  }

  public static ServerStoreConfiguration getServerStoreConfiguration(String resourceName) {
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(resourceName, 8, MB);
    return new ServerStoreConfiguration(resourcePool.getPoolAllocation(),
        String.class.getName(), String.class.getName(), CompactJavaSerializer.class.getName(), CompactJavaSerializer.class
        .getName(), Consistency.STRONG, false);
  }
}
