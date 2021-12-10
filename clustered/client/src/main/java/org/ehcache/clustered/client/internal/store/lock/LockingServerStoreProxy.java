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
package org.ehcache.clustered.client.internal.store.lock;

import org.ehcache.clustered.client.internal.store.ServerStoreProxy;

import java.util.concurrent.TimeoutException;

public interface LockingServerStoreProxy extends ServerStoreProxy {

  /**
   *
   * @param hash
   */
  ChainEntry lock(long hash) throws TimeoutException;

  /**
   *
   * @param hash
   * @param localonly
   */
  void unlock(long hash, boolean localonly) throws TimeoutException;
}
