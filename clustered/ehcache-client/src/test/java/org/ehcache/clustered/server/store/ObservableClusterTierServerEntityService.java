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

package org.ehcache.clustered.server.store;

import org.ehcache.clustered.common.internal.messages.CommonConfigCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EntityConfigurationCodec;
import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.ehcache.clustered.server.ClusterTierManagerActiveEntity;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

public class ObservableClusterTierServerEntityService
    implements EntityServerService<EhcacheEntityMessage, EhcacheEntityResponse> {
  private final ClusterTierServerEntityService delegate = new ClusterTierServerEntityService();

  private final Map<String, List<ClusterTierActiveEntity>> servedActiveEntities = new ConcurrentHashMap<>();
  private final Map<String, List<ClusterTierPassiveEntity>> servedPassiveEntities = new ConcurrentHashMap<>();

  public List<ObservableClusterTierActiveEntity> getServedActiveEntities() throws NoSuchFieldException, IllegalAccessException {
    List<ObservableClusterTierActiveEntity> observables = new ArrayList<>(servedActiveEntities.size());
    for (String name : servedActiveEntities.keySet()) {
      observables.addAll(getServedActiveEntitiesFor(name));
    }
    return Collections.unmodifiableList(observables);
  }

  public List<ObservableClusterTierActiveEntity> getServedActiveEntitiesFor(String name) {
    List<ClusterTierActiveEntity> entities = servedActiveEntities.get(name);
    if (entities == null) {
      return emptyList();
    } else {
      List<ObservableClusterTierActiveEntity> observables = new ArrayList<>(entities.size());
      for (ClusterTierActiveEntity entity : entities) {
        observables.add(new ObservableClusterTierActiveEntity(entity));
      }
      return Collections.unmodifiableList(observables);
    }
  }

  public List<ObservableClusterTierPassiveEntity> getServedPassiveEntities() throws Exception {
    List<ObservableClusterTierPassiveEntity> observables = new ArrayList<>(servedPassiveEntities.size());
    for (String name : servedPassiveEntities.keySet()) {
      observables.addAll(getServedPassiveEntitiesFor(name));
    }
    return Collections.unmodifiableList(observables);
  }

  public List<ObservableClusterTierPassiveEntity> getServedPassiveEntitiesFor(String name) throws Exception {
    List<ClusterTierPassiveEntity> entities = servedPassiveEntities.get(name);
    if (entities == null) {
      return emptyList();
    } else {
      List<ObservableClusterTierPassiveEntity> observables = new ArrayList<>(entities.size());
      for (ClusterTierPassiveEntity entity : entities) {
        observables.add(new ObservableClusterTierPassiveEntity(entity));
      }
      return Collections.unmodifiableList(observables);
    }
  }

  @Override
  public long getVersion() {
    return delegate.getVersion();
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return delegate.handlesEntityType(typeName);
  }

  @Override
  public ClusterTierActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ClusterTierEntityConfiguration c = new EntityConfigurationCodec(new CommonConfigCodec()).decodeClusteredStoreConfiguration(configuration);
    ClusterTierActiveEntity activeEntity = delegate.createActiveEntity(registry, configuration);
    List<ClusterTierActiveEntity> existing = servedActiveEntities.putIfAbsent(c.getStoreIdentifier(), new ArrayList<>(singleton(activeEntity)));
    if (existing != null) {
      existing.add(activeEntity);
    }
    return activeEntity;
  }

  @Override
  public ClusterTierPassiveEntity createPassiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ClusterTierEntityConfiguration c = new EntityConfigurationCodec(new CommonConfigCodec()).decodeClusteredStoreConfiguration(configuration);
    ClusterTierPassiveEntity passiveEntity = delegate.createPassiveEntity(registry, configuration);
    List<ClusterTierPassiveEntity> existing = servedPassiveEntities.putIfAbsent(c.getStoreIdentifier(), new ArrayList<>(singleton(passiveEntity)));
    if (existing != null) {
      existing.add(passiveEntity);
    }
    return passiveEntity;
  }

  @Override
  public ConcurrencyStrategy<EhcacheEntityMessage> getConcurrencyStrategy(byte[] config) {
    return delegate.getConcurrencyStrategy(config);
  }

  @Override
  public ExecutionStrategy<EhcacheEntityMessage> getExecutionStrategy(byte[] configuration) {
    return delegate.getExecutionStrategy(configuration);
  }

  @Override
  public MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> getMessageCodec() {
    return delegate.getMessageCodec();
  }

  @Override
  public SyncMessageCodec<EhcacheEntityMessage> getSyncMessageCodec() {
    return delegate.getSyncMessageCodec();
  }

  /**
   * Provides access to unit test state methods in an {@link ClusterTierManagerActiveEntity} instance.
   */
  public static final class ObservableClusterTierActiveEntity {
    private final ClusterTierActiveEntity activeEntity;

    private ObservableClusterTierActiveEntity(ClusterTierActiveEntity activeEntity) {
      this.activeEntity = activeEntity;
    }

    public ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> getActiveEntity() {
      return this.activeEntity;
    }

    public void notifyDestroyed(ClientSourceId sourceId) {
      activeEntity.notifyDestroyed(sourceId);
    }

    public Set<ClientDescriptor> getConnectedClients() {
      return activeEntity.getConnectedClients();
    }

    @SuppressWarnings("unchecked")
    public ConcurrentMap<Integer, ClusterTierActiveEntity.InvalidationHolder> getClientsWaitingForInvalidation() throws Exception {
      Field field = activeEntity.getClass().getDeclaredField("clientsWaitingForInvalidation");
      field.setAccessible(true);
      return (ConcurrentMap<Integer, ClusterTierActiveEntity.InvalidationHolder>) field.get(activeEntity);
    }

    @SuppressWarnings("unchecked")
    public OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> getMessageHandler() throws Exception {
      Field field = activeEntity.getClass().getDeclaredField("messageHandler");
      field.setAccessible(true);
      return (OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse>) field.get(activeEntity);
    }

  }

  public static final class ObservableClusterTierPassiveEntity {
    private final ClusterTierPassiveEntity passiveEntity;

    private ObservableClusterTierPassiveEntity(ClusterTierPassiveEntity passiveEntity) throws Exception {
      this.passiveEntity = passiveEntity;
      Field field = passiveEntity.getClass().getDeclaredField("stateService");
      field.setAccessible(true);
    }

    public void notifyDestroyed(ClientSourceId sourceId) {
      passiveEntity.notifyDestroyed(sourceId);
    }

    @SuppressWarnings("unchecked")
    public OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> getMessageHandler() throws Exception {
      Field field = passiveEntity.getClass().getDeclaredField("messageHandler");
      field.setAccessible(true);
      return (OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse>) field.get(passiveEntity);
    }

  }
}
