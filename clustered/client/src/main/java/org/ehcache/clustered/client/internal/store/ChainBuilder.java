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

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.Util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds {@link Chain}s
 */
public class ChainBuilder {

  private List<ByteBuffer> buffers = new ArrayList<>();

  public ChainBuilder() {
  }

  private ChainBuilder(List<ByteBuffer> buffers) {
    this.buffers = buffers;
  }

  //TODO: optimize this & make this mutable
  public ChainBuilder add(final ByteBuffer payload) {
    List<ByteBuffer> newList = new ArrayList<>(buffers.size() + 1);
    newList.addAll(buffers);
    newList.add(payload);
    return new ChainBuilder(newList);
  }

  public Chain build() {
    ByteBuffer[] elements = new ByteBuffer[buffers.size()];
    buffers.toArray(elements);
    return Util.getChain(false, elements);
  }

}
