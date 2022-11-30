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
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.StateDumpCollector;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides an alternative to {@link ClusterTierManagerServerEntityService} for unit tests to enable observing
 * the state of the {@link ClusterTierManagerActiveEntity} instances served.
 */
public class ObservableEhcacheServerEntityService extends ClusterTierManagerServerEntityService {

  private final List<ObservableEhcacheActiveEntity> servedActiveEntities = new ArrayList<>();

  /**
   * Gets a list of {@link ObservableEhcacheActiveEntity} instances wrapping the
   * {@link ClusterTierManagerActiveEntity} instances served by this {@link EntityServerService}.
   *
   * @return an unmodifiable list of {@code ObservableEhcacheActiveEntity} instances
   */
  public List<ObservableEhcacheActiveEntity> getServedActiveEntities() {
    return Collections.unmodifiableList(servedActiveEntities);
  }

  @Override
  public ClusterTierManagerActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ObservableEhcacheActiveEntity activeEntity = new ObservableEhcacheActiveEntity(super.createActiveEntity(registry, configuration));
    servedActiveEntities.add(activeEntity);
    return activeEntity;
  }

  /**
   * Provides access to unit test state methods in an {@link ClusterTierManagerActiveEntity} instance.
   */
  public static final class ObservableEhcacheActiveEntity extends ClusterTierManagerActiveEntity {
    private final EhcacheStateServiceImpl ehcacheStateService;
    private final ClusterTierManagerActiveEntity activeEntity;
    private final Set<ClientDescriptor> connectedClients = ConcurrentHashMap.newKeySet();

    ObservableEhcacheActiveEntity(ClusterTierManagerActiveEntity activeEntity) {
      this.activeEntity = activeEntity;
      try {
        Field field = ClusterTierManagerActiveEntity.class.getDeclaredField("ehcacheStateService");
        field.setAccessible(true);
        this.ehcacheStateService = (EhcacheStateServiceImpl) field.get(activeEntity);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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

    @Override
    public void addStateTo(StateDumpCollector dump) {
      activeEntity.addStateTo(dump);
    }

    @Override
    public void connected(ClientDescriptor clientDescriptor) {
      connectedClients.add(clientDescriptor);
      activeEntity.connected(clientDescriptor);
    }

    @Override
    public void disconnected(ClientDescriptor clientDescriptor) {
      connectedClients.remove(clientDescriptor);
      activeEntity.disconnected(clientDescriptor);
    }

    @Override
    public EhcacheEntityResponse invokeActive(ActiveInvokeContext<EhcacheEntityResponse> invokeContext, EhcacheEntityMessage message) {
      return activeEntity.invokeActive(invokeContext, message);
    }

    @Override
    public ReconnectHandler startReconnect() {
      return activeEntity.startReconnect();
    }

    @Override
    public void synchronizeKeyToPassive(PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel, int concurrencyKey) {
      activeEntity.synchronizeKeyToPassive(syncChannel, concurrencyKey);
    }

    @Override
    public void createNew() {
      activeEntity.createNew();
    }

    @Override
    public void loadExisting() {
      activeEntity.loadExisting();
    }

    @Override
    public void destroy() {
      activeEntity.destroy();
    }

    @Override
    public void notifyDestroyed(ClientSourceId sourceId) {
      activeEntity.notifyDestroyed(sourceId);
    }

    public Set<ClientDescriptor> getConnectedClients() {
      return Collections.unmodifiableSet(connectedClients);
    }
  }

}
