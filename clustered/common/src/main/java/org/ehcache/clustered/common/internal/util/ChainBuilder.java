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
package org.ehcache.clustered.common.internal.util;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Builds {@link Chain}s
 */
public class ChainBuilder {

  private List<ByteBuffer> buffers = new ArrayList<>();

  public ChainBuilder add(final ByteBuffer payload) {
    buffers.add(payload);
    return this;
  }

  public Chain build() {
    List<Element> elements = new ArrayList<>();
    for (final ByteBuffer buffer : buffers) {
      elements.add(buffer::asReadOnlyBuffer);
    }
    return chainFromList(elements);
  }

  public int length() {
    return buffers.size();
  }

  public static Chain chainFromList(List<Element> elements) {
    return new Chain() {
      @Override
      public boolean isEmpty() {
        return elements.isEmpty();
      }

      @Override
      public int length() {
        return elements.size();
      }

      @Override
      public Iterator<Element> iterator() {
        return elements.iterator();
      }
    };
  }
}
