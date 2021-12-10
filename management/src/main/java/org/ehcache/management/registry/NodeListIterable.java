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
package org.ehcache.management.registry;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Iterator;
import java.util.NoSuchElementException;

class NodeListIterable<T extends Node> implements Iterable<T> {
  private final NodeList nodeList;
  private final Class<T> type;

  private NodeListIterable(NodeList nodeList, Class<T> type) {
    this.nodeList = nodeList;
    this.type = type;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      int i = 0;

      @Override
      public boolean hasNext() {
        return nodeList != null && i < nodeList.getLength();
      }

      @Override
      public T next() throws NoSuchElementException {
        Node node;
        if (!hasNext() || (node = nodeList.item(i++)) == null) {
          throw new NoSuchElementException();
        }
        return type.cast(node);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  static Iterable<Element> elements(Element root, String ns, String localName) {
    return new NodeListIterable<>(root.getElementsByTagNameNS(ns, localName), Element.class);
  }

}
