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
package org.ehcache.clustered.server.offheap;

import java.nio.ByteBuffer;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;
import org.ehcache.clustered.server.store.ChainBuilder;
import org.ehcache.clustered.server.store.ElementBuilder;
import org.ehcache.clustered.server.store.ServerStore;
import org.ehcache.clustered.server.store.ServerStoreTest;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

public class OffHeapServerStoreTest extends ServerStoreTest {

  @Override
  public ServerStore newStore() {
    return new OffHeapServerStore(new UnlimitedPageSource(new OffHeapBufferSource()), 16);
  }

  @Override
  public ChainBuilder newChainBuilder() {
    return new ChainBuilder() {
      @Override
      public Chain build(Element... elements) {
        ByteBuffer[] buffers = new ByteBuffer[elements.length];
        for (int i = 0; i < buffers.length; i++) {
          buffers[i] = elements[i].getPayload();
        }
        return OffHeapChainMap.chain(buffers);
      }
    };
  }

  @Override
  public ElementBuilder newElementBuilder() {
    return new ElementBuilder() {
      @Override
      public Element build(final ByteBuffer payLoad) {
        return new Element() {
          @Override
          public ByteBuffer getPayload() {
            return payLoad;
          }
        };
      }
    };
  }

}
