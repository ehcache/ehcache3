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
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides an alternative to {@link EhcacheServerEntityService} for unit tests to enable observing
 * the state of the {@link EhcacheActiveEntity} instances served.
 */
public class ObservableEhcacheServerEntityService
    implements EntityServerService<EhcacheEntityMessage, EhcacheEntityResponse> {
  private final EhcacheServerEntityService delegate = new EhcacheServerEntityService();

  private final List<EhcacheActiveEntity> servedActiveEntities = new ArrayList<EhcacheActiveEntity>();

  /**
   * Gets a list of {@link ObservableEhcacheActiveEntity} instances wrapping the
   * {@link EhcacheActiveEntity} instances served by this {@link EntityServerService}.
   *
   * @return an unmodifiable list of {@code ObservableEhcacheActiveEntity} instances
   */
  public List<ObservableEhcacheActiveEntity> getServedActiveEntities() throws NoSuchFieldException, IllegalAccessException {
    List<ObservableEhcacheActiveEntity> observables = new ArrayList<ObservableEhcacheActiveEntity>(servedActiveEntities.size());
    for (EhcacheActiveEntity servedActiveEntity : servedActiveEntities) {
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
  public EhcacheActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) {
    EhcacheActiveEntity activeEntity = delegate.createActiveEntity(registry, configuration);
    servedActiveEntities.add(activeEntity);
    return activeEntity;
  }

  @Override
  public PassiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> createPassiveEntity(ServiceRegistry registry, byte[] configuration) {
    return delegate.createPassiveEntity(registry, configuration);
  }

  @Override
  public ConcurrencyStrategy<EhcacheEntityMessage> getConcurrencyStrategy(byte[] config) {
    return delegate.getConcurrencyStrategy(config);
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
   * Provides access to unit test state methods in an {@link EhcacheActiveEntity} instance.
   */
  public static final class ObservableEhcacheActiveEntity {
    private final EhcacheActiveEntity activeEntity;
    private final EhcacheStateServiceImpl ehcacheStateService;

    private ObservableEhcacheActiveEntity(EhcacheActiveEntity activeEntity) throws NoSuchFieldException, IllegalAccessException {
      this.activeEntity = activeEntity;
      Field field = activeEntity.getClass().getDeclaredField("ehcacheStateService");
      field.setAccessible(true);
      this.ehcacheStateService = (EhcacheStateServiceImpl)field.get(activeEntity);
    }

    public ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> getActiveEntity() {
      return this.activeEntity;
    }

    public Map<ClientDescriptor, Set<String>> getConnectedClients() {
      return activeEntity.getConnectedClients();
    }

    public Set<String> getStores() {
      return ehcacheStateService.getStores();
    }

    public Map<String, Set<ClientDescriptor>> getInUseStores() {
      return activeEntity.getInUseStores();
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
