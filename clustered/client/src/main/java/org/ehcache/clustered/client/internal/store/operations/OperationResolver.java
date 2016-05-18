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

package org.ehcache.clustered.client.internal.store.operations;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class OperationResolver<K, V> {

  private final OperationsCodec<K, V> codec;

  public OperationResolver(final OperationsCodec<K, V> codec) {
    this.codec = codec;
  }

  /**
   * Extract the {@code Element}s from the provided {@code Chain} that are not associated with the provided key
   * and create a new {@code Chain}
   *
   * Separate the {@code Element}s from the provided {@code Chain} that are associated and not associated with
   * the provided key. Create a new chain with the unassociated {@code Element}s. Resolve the associated elements
   * and append the resolved {@code Element} to the newly created chain.
   *
   * @param chain a heterogeneous {@code Chain}
   * @param key a key
   * @return an entry with the resolved operation for the provided key as the key
   * and the compacted chain as the value
   * @throws ClassNotFoundException
   */
  @SuppressFBWarnings({ "DLS_DEAD_LOCAL_STORE", "UC_USELESS_OBJECT" })
  public Map.Entry<Operation<K>, Chain> resolve(Chain chain, K key) {
    Operation<K> resolvedOperation = null;
    List<Element> elements = new ArrayList<Element>();
    for (Element element : chain) {
      ByteBuffer payload = element.getPayload();
      Operation<K> operation = codec.decode(payload);
      if(key.equals(operation.getKey())) {
        resolvedOperation = operation.apply(resolvedOperation);
      } else {
        elements.add(element);
      }
    }
    ByteBuffer payload = codec.encode(resolvedOperation);
    Element resolvedElement = null; // TODO: 13/05/16 build an element using the payload
    elements.add(resolvedElement);
    Chain newChain = null;  // TODO: 13/05/16  build the chain using the list of elements and the resolvedElement
    return new AbstractMap.SimpleEntry<Operation<K>, Chain>(resolvedOperation, newChain);
  }
}
