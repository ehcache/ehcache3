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
package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.config.Timeouts;

import java.util.concurrent.Executor;

/**
 * ClusterTierUserData
 *
 * Additional information passed to client side cluster tier entity.
 */
public class ClusterTierUserData {
  private final Timeouts timeouts;
  private final String storeIdentifier;
  private final Executor asyncWorker;

  public ClusterTierUserData(Timeouts timeouts, String storeIdentifier, Executor asyncWorker) {
    this.timeouts = timeouts;
    this.storeIdentifier = storeIdentifier;
    this.asyncWorker = asyncWorker;
  }

  public Timeouts getTimeouts() {
    return timeouts;
  }

  public String getStoreIdentifier() {
    return storeIdentifier;
  }

  public Executor getAsyncWorker() {
    return asyncWorker;
  }
}
