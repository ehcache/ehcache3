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
package org.ehcache.loaderwriter.writebehind.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Geert Bevin
 * @author Chris Dennis
 *
 */
public class CoalesceKeysFilter<K, V> {

  public void filter(List<SingleOperation<K, V>> operations) {

    final Map<K, KeyBasedOperation<K>> mostRecent = new HashMap<K, KeyBasedOperation<K>>();
    final List<KeyBasedOperation<K>> operationsToRemove = new ArrayList<KeyBasedOperation<K>>();
    
    for (KeyBasedOperation<K> keyBasedOperation : operations) {
      if(!mostRecent.containsKey(keyBasedOperation.getKey())) {
        mostRecent.put(keyBasedOperation.getKey(), keyBasedOperation);
      }
      else {
        KeyBasedOperation<K> previousOperation = mostRecent.get(keyBasedOperation.getKey());
        if(previousOperation.getCreationTime() > keyBasedOperation.getCreationTime()) {
          operationsToRemove.add(keyBasedOperation);
        }
        else {
          operationsToRemove.add(previousOperation);
          mostRecent.put(keyBasedOperation.getKey(), keyBasedOperation);
        }
      }
    }

    operations.removeAll(operationsToRemove);

  }

}
