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

import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.server.ClusterTierManagerActiveEntity;
import org.ehcache.clustered.server.EhcacheStateServiceImpl;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ObservableClusterTierServerEntityService
    implements EntityServerService<EhcacheEntityMessage, EhcacheEntityResponse> {
  private final ClusterTierServerEntityService delegate = new ClusterTierServerEntityService();

  private final List<ClusterTierActiveEntity> servedActiveEntities = new ArrayList<>();
  private final List<ClusterTierPassiveEntity> servedPassiveEntities = new ArrayList<>();

  public List<ObservableClusterTierActiveEntity> getServedActiveEntities() throws NoSuchFieldException, IllegalAccessException {
    List<ObservableClusterTierActiveEntity> observables = new ArrayList<ObservableClusterTierActiveEntity>(servedActiveEntities.size());
    for (ClusterTierActiveEntity servedActiveEntity : servedActiveEntities) {
      observables.add(new ObservableClusterTierActiveEntity(servedActiveEntity));
    }
    return Collections.unmodifiableList(observables);
  }

  public List<ObservableClusterTierPassiveEntity> getServedPassiveEntities() throws Exception {
    List<ObservableClusterTierPassiveEntity> observables = new ArrayList<>(servedPassiveEntities.size());
    for (ClusterTierPassiveEntity servedPassiveEntity : servedPassiveEntities) {
      observables.add(new ObservableClusterTierPassiveEntity(servedPassiveEntity));
    }
    return Collections.unmodifiableList(observables);
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
    ClusterTierActiveEntity activeEntity = delegate.createActiveEntity(registry, configuration);
    servedActiveEntities.add(activeEntity);
    return activeEntity;
  }

  @Override
  public ClusterTierPassiveEntity createPassiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ClusterTierPassiveEntity passiveEntity = delegate.createPassiveEntity(registry, configuration);
    servedPassiveEntities.add(passiveEntity);
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

    private ObservableClusterTierActiveEntity(ClusterTierActiveEntity activeEntity) throws NoSuchFieldException, IllegalAccessException {
      this.activeEntity = activeEntity;
    }

    public ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> getActiveEntity() {
      return this.activeEntity;
    }

    public Set<ClientDescriptor> getConnectedClients() {
      return activeEntity.getConnectedClients();
    }

    public Set<ClientDescriptor> getAttachedClients() {
      return activeEntity.getAttachedClients();
    }

    public Map getClientsWaitingForInvalidation() throws Exception {
      Field field = activeEntity.getClass().getDeclaredField("clientsWaitingForInvalidation");
      field.setAccessible(true);
      return (Map)field.get(activeEntity);
    }
  }

  public static final class ObservableClusterTierPassiveEntity {
    private final ClusterTierPassiveEntity passiveEntity;
    private final EhcacheStateServiceImpl ehcacheStateService;

    private ObservableClusterTierPassiveEntity(ClusterTierPassiveEntity passiveEntity) throws Exception {
      this.passiveEntity = passiveEntity;
      Field field = passiveEntity.getClass().getDeclaredField("stateService");
      field.setAccessible(true);
      this.ehcacheStateService = (EhcacheStateServiceImpl)field.get(passiveEntity);
    }

    public Map getMessageTrackerMap(String storeAlias) throws Exception {
      Field field = this.ehcacheStateService.getClientMessageTracker(storeAlias).getClass().getDeclaredField("clientUUIDMessageTrackerMap");
      field.setAccessible(true);
      return (Map)field.get(this.ehcacheStateService.getClientMessageTracker(storeAlias));
    }

  }
}
