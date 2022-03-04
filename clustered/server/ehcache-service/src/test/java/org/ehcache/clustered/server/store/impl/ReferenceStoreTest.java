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
package org.ehcache.clustered.server.store.impl;

import org.ehcache.clustered.server.store.ChainBuilder;
import org.ehcache.clustered.server.store.ElementBuilder;
import org.ehcache.clustered.common.internal.store.ServerStore;
import org.ehcache.clustered.server.store.ServerStoreTest;

/**
 * Tests {@link ReferenceStoreImpl}
 */
public class ReferenceStoreTest extends ServerStoreTest {
  @Override
  public ServerStore newStore() {
    return new ReferenceStoreImpl();
  }

  @Override
  public ChainBuilder newChainBuilder() {
    return new HeapChainBuilder();
  }

  @Override
  public ElementBuilder newElementBuilder() {
    return new HeapElementBuilder();
  }
}
