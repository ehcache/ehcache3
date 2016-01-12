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

import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServerEntityService;
import org.terracotta.entity.ServiceRegistry;

public class EhcacheServerEntityService implements ServerEntityService<EhcacheActiveEntity, PassiveServerEntity<EntityMessage, EntityResponse>>{

  @Override
  public long getVersion() {
    return 0L;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return "org.ehcache.clustered.client.EhcacheClientEntity".equals(typeName);
  }

  @Override
  public EhcacheActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) {
    return new EhcacheActiveEntity(configuration);
  }

  @Override
  public PassiveServerEntity<EntityMessage, EntityResponse> createPassiveEntity(ServiceRegistry registry, byte[] configuration) {
    throw new UnsupportedOperationException("Active/passive is not supported yet");
  }
  
}
