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

package org.ehcache.core.util;

import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author Chris Dennis
 */
public class KeysIterable<K> implements Iterable<K> {

  private final Iterable<? extends Map.Entry<? extends K, ?>> entries;

  public static <K> Iterable<K> keysOf(Iterable<? extends Map.Entry<? extends K, ?>> entries) {
    return new KeysIterable<K>(entries);
  }
  
  private KeysIterable(Iterable<? extends Map.Entry<? extends K, ?>> entries) {
    this.entries = entries;
  }

  @Override
  public Iterator<K> iterator() {
    return new KeysIterator(entries.iterator());
  }

  private class KeysIterator implements Iterator<K> {

    private final Iterator<? extends Map.Entry<? extends K, ?>> entriesIterator;

    public KeysIterator(Iterator<? extends Map.Entry<? extends K, ?>> entriesIterator) {
      this.entriesIterator = entriesIterator;
    }

    @Override
    public boolean hasNext() {
      return entriesIterator.hasNext();
    }

    @Override
    public K next() {
      return entriesIterator.next().getKey();
    }

    @Override
    public void remove() {
      entriesIterator.remove();
    }
  }

}
