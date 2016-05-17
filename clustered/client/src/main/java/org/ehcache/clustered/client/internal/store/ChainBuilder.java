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

import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Builds {@link Chain}s
 */
public class ChainBuilder {

  private List<Element> elements = new ArrayList<Element>();

  public ChainBuilder() {

  }

  private ChainBuilder(List<Element> elements) {
    this.elements = elements;
  }

  public ChainBuilder add(final ByteBuffer payload) {
    this.elements.add(new Element() {
      @Override
      public ByteBuffer getPayload() {
        return payload;
      }
    });
    return new ChainBuilder(this.elements);
  }

  public Chain build() {
    return new DummyChain(elements);
  }

  private static class DummyChain implements Chain {

    private final List<Element> elementList;

    private DummyChain(List<Element> elements) {
      elementList = elements;
    }

    @Override
    public Iterator<Element> reverseIterator() {
      throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public boolean isEmpty() {
      return elementList.isEmpty();
    }

    @Override
    public Iterator<Element> iterator() {
      return elementList.iterator();
    }
  }
}
