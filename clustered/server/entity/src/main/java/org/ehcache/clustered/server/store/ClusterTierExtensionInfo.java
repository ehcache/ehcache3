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

import org.ehcache.clustered.common.internal.util.ExtensionInfoSkeleton;

import static org.ehcache.clustered.common.internal.util.ExtensionInfoSkeleton.Type.ENTITY;

/**
 * @author Mathieu Carbou
 */
public class ClusterTierExtensionInfo extends ExtensionInfoSkeleton {

  @Override
  protected String getName() {
    return "Ehcache Cluster Tier";
  }

  @Override
  protected Type getType() {
    return ENTITY;
  }
}
