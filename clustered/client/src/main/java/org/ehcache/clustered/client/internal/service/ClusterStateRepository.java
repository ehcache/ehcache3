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

package org.ehcache.clustered.client.internal.service;

import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.spi.persistence.StateHolder;
import org.ehcache.spi.persistence.StateRepository;

import java.io.Serializable;

/**
 * ClusterStateRepository
 */
class ClusterStateRepository implements StateRepository {

  private final ClusteringService.ClusteredCacheIdentifier clusterCacheIdentifier;
  private final ClusterTierClientEntity clientEntity;
  private final String composedId;

  ClusterStateRepository(ClusteringService.ClusteredCacheIdentifier clusterCacheIdentifier, String id, ClusterTierClientEntity clientEntity) {
    this.clusterCacheIdentifier = clusterCacheIdentifier;
    this.composedId = clusterCacheIdentifier.getId() + "-" + id;
    this.clientEntity = clientEntity;
  }

  @Override
  public <K extends Serializable, V extends Serializable> StateHolder<K, V> getPersistentStateHolder(String name, Class<K> keyClass, Class<V> valueClass) {
    return new ClusteredStateHolder<K, V>(clusterCacheIdentifier.getId(), composedId + "-" + name, clientEntity, keyClass, valueClass);
  }
}
