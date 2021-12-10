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

package org.ehcache.clustered.server.state.config;

import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.terracotta.entity.ServiceConfiguration;

public class EhcacheStoreStateServiceConfig implements ServiceConfiguration<EhcacheStateService> {

  private final String managerIdentifier;
  private final KeySegmentMapper mapper;


  public EhcacheStoreStateServiceConfig(String managerIdentifier, final KeySegmentMapper mapper) {
    this.managerIdentifier = managerIdentifier;
    this.mapper = mapper;
  }

  @Override
  public Class<EhcacheStateService> getServiceType() {
    return EhcacheStateService.class;
  }

  public String getManagerIdentifier() {
    return managerIdentifier;
  }

  public KeySegmentMapper getMapper() {
    return mapper;
  }
}
