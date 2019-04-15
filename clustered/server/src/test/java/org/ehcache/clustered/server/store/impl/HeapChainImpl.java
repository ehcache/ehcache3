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

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Implements {@link Chain}
 */
public class HeapChainImpl implements Chain {

  private Node first;
  private Node last;

  public HeapChainImpl(Element... elements) {
    createChain(elements);
  }

  @Override
  public boolean isEmpty() {
    return first == null;
  }

  @Override
  public int length() {
    int length = 0;
    while (iterator().hasNext()) {
      iterator().next();
      length++;
    }
    return length;
  }

  @Override
  public Iterator<Element> iterator() {
    return new ChainIterator();
  }

  /**
   *
   * @param element to be appended to the chain
   */
  Chain append(Element element) {
    List<Element> presentElements = new LinkedList<>();
    for (Element l : this) {
      presentElements.add(l);
    }
    presentElements.add(element);
    Element[] elementArr = new Element[presentElements.size()];
    return new HeapChainImpl(presentElements.toArray(elementArr));
  }

  private void createChain(Element... elements) {

    if (elements != null && elements.length != 0) {
      List<Element> reordered = Arrays.asList(elements);
      Collections.sort(reordered, (o1, o2) -> {
        HeapElementImpl oh1 = (HeapElementImpl)o1;
        HeapElementImpl oh2 = (HeapElementImpl)o2;
        if (oh1.getSequenceNumber() > oh2.getSequenceNumber()) {
          return 1;
        } else if (oh1.getSequenceNumber() < oh2.getSequenceNumber()) {
          return -1;
        } else {
          return 0;
        }
      });
      List<Element> sortedList = new ArrayList<>(reordered);
      if (!sortedList.isEmpty()) {
        createFirst(sortedList.remove(0));
      }

      for (Element element : sortedList) {
        add(element);
      }
    }
  }

  private void createFirst(Element element) {
    Node newNode = new Node(element);
    this.first = newNode;
    this.last = newNode;
  }

  private void add(Element element) {
    Node newNode = new Node(element);
    this.last.nextLink = newNode;
    newNode.prevLink = this.last;
    this.last = newNode;
  }

  private class ChainIterator implements Iterator<Element> {

    private Node current;

    ChainIterator() {
      this.current = first;
    }

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public Element next() {
      Node temp = this.current;
      this.current = this.current.nextLink;
      return temp.element;
    }
  }

  private static class Node {
    Element element;
    Node nextLink;
    Node prevLink;

    Node (Element element) {
      this.element = element;
    }
  }
}
