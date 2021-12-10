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
package org.ehcache.clustered.server.management;

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.terracotta.management.service.monitoring.registry.provider.AliasBinding;

import java.util.Objects;

class PoolBinding extends AliasBinding {

  enum AllocationType {
    SHARED,
    DEDICATED
  }

  // this marker is used to send global notification - it is not a real pool
  static final PoolBinding ALL_SHARED = new PoolBinding("PoolBinding#all-shared", new ServerSideConfiguration.Pool(1, ""), AllocationType.SHARED);

  private final AllocationType allocationType;

  PoolBinding(String identifier, ServerSideConfiguration.Pool serverStore, AllocationType allocationType) {
    super(identifier, serverStore);
    this.allocationType = Objects.requireNonNull(allocationType);
  }

  AllocationType getAllocationType() {
    return allocationType;
  }

  @Override
  public ServerSideConfiguration.Pool getValue() {
    return (ServerSideConfiguration.Pool) super.getValue();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    PoolBinding that = (PoolBinding) o;
    return allocationType == that.allocationType;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + allocationType.hashCode();
    return result;
  }

}
