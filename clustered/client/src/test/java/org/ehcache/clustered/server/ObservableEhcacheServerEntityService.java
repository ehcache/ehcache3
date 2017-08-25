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

package org.ehcache.clustered.server;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
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
import java.util.Set;

/**
 * Provides an alternative to {@link ClusterTierManagerServerEntityService} for unit tests to enable observing
 * the state of the {@link ClusterTierManagerActiveEntity} instances served.
 */
public class ObservableEhcacheServerEntityService
    implements EntityServerService<EhcacheEntityMessage, EhcacheEntityResponse> {
  private final ClusterTierManagerServerEntityService delegate = new ClusterTierManagerServerEntityService();

  private final List<ClusterTierManagerActiveEntity> servedActiveEntities = new ArrayList<ClusterTierManagerActiveEntity>();
  private final List<ClusterTierManagerPassiveEntity> servedPassiveEntities = new ArrayList<>();

  /**
   * Gets a list of {@link ObservableEhcacheActiveEntity} instances wrapping the
   * {@link ClusterTierManagerActiveEntity} instances served by this {@link EntityServerService}.
   *
   * @return an unmodifiable list of {@code ObservableEhcacheActiveEntity} instances
   */
  public List<ObservableEhcacheActiveEntity> getServedActiveEntities() throws NoSuchFieldException, IllegalAccessException {
    List<ObservableEhcacheActiveEntity> observables = new ArrayList<ObservableEhcacheActiveEntity>(servedActiveEntities.size());
    for (ClusterTierManagerActiveEntity servedActiveEntity : servedActiveEntities) {
      observables.add(new ObservableEhcacheActiveEntity(servedActiveEntity));
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
  public ClusterTierManagerActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ClusterTierManagerActiveEntity activeEntity = delegate.createActiveEntity(registry, configuration);
    servedActiveEntities.add(activeEntity);
    return activeEntity;
  }

  @Override
  public ClusterTierManagerPassiveEntity createPassiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ClusterTierManagerPassiveEntity passiveEntity = delegate.createPassiveEntity(registry, configuration);
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
  public static final class ObservableEhcacheActiveEntity {
    private final ClusterTierManagerActiveEntity activeEntity;
    private final EhcacheStateServiceImpl ehcacheStateService;

    private ObservableEhcacheActiveEntity(ClusterTierManagerActiveEntity activeEntity) throws NoSuchFieldException, IllegalAccessException {
      this.activeEntity = activeEntity;
      Field field = activeEntity.getClass().getDeclaredField("ehcacheStateService");
      field.setAccessible(true);
      this.ehcacheStateService = (EhcacheStateServiceImpl)field.get(activeEntity);
    }

    public ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> getActiveEntity() {
      return this.activeEntity;
    }

    public Set<ClientDescriptor> getConnectedClients() {
      return activeEntity.getConnectedClients();
    }

    public Set<String> getStores() {
      return ehcacheStateService.getStores();
    }

    public String getDefaultServerResource() {
      return ehcacheStateService.getDefaultServerResource();
    }

    public Set<String> getSharedResourcePoolIds() {
      return ehcacheStateService.getSharedResourcePoolIds();
    }

    public Set<String> getDedicatedResourcePoolIds() {
      return ehcacheStateService.getDedicatedResourcePoolIds();
    }

  }

}
