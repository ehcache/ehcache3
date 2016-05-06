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

import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.terracotta.offheapstore.paging.PageSource;

class ServerStore {

  private final ServerStoreConfiguration storeConfiguration;
  private final PageSource pageSource;

  public ServerStore(ServerStoreConfiguration storeConfiguration, PageSource pageSource) {
    this.storeConfiguration = storeConfiguration;
    this.pageSource = pageSource;
  }

  /**
   * Gets the {@link PageSource} providing storage for this {@code ServerStore}.
   *
   * @return the {@code PageSource} used by this {@code ServerStore}
   */
  PageSource getPageSource() {
    return pageSource;
  }

  public ServerStoreConfiguration getStoreConfiguration() {
    return storeConfiguration;
  }
}
