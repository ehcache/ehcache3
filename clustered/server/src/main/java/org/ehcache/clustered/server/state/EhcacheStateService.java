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

package org.ehcache.clustered.server.state;

import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ConfigureStoreManager;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateStoreManager;
import org.ehcache.clustered.server.ServerStoreImpl;
import org.ehcache.clustered.server.repo.StateRepositoryManager;

import com.tc.classloader.CommonComponent;

import java.util.Set;

@CommonComponent
public interface EhcacheStateService {

  ServerStoreImpl getStore(String name);

  Set<String> getStores();

  void destroy();

  void validate(ValidateStoreManager message) throws ClusterException;

  void configure(ConfigureStoreManager message) throws ClusterException;

  ServerStoreImpl createStore(String name, ServerStoreConfiguration serverStoreConfiguration) throws ClusterException;

  void destroyServerStore(String name) throws ClusterException;

  boolean isConfigured();

  StateRepositoryManager getStateRepositoryManager() throws ClusterException;

}
