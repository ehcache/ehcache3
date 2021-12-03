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
package org.ehcache.clustered.client.internal.loaderwriter.writebehind;

import org.ehcache.clustered.common.internal.util.ChainBuilder;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.common.internal.store.operations.ConditionalRemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.Operation;
import org.ehcache.clustered.common.internal.store.operations.PutOperation;
import org.ehcache.clustered.common.internal.store.operations.RemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

class ClusteredWriteBehind<K, V> {
  private final ClusteredWriteBehindStore<K, V> clusteredWriteBehindStore;
  private final ExecutorService executorService;
  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final OperationsCodec<K, V> codec;
  private final ChainResolver<K, V> resolver;

  ClusteredWriteBehind(ClusteredWriteBehindStore<K, V> clusteredWriteBehindStore,
                       ExecutorService executorService,
                       ChainResolver<K, V> resolver,
                       CacheLoaderWriter<? super K, V> cacheLoaderWriter,
                       OperationsCodec<K, V> codec) {
    this.clusteredWriteBehindStore = clusteredWriteBehindStore;
    this.executorService = executorService;
    this.resolver = resolver;
    this.cacheLoaderWriter = cacheLoaderWriter;
    this.codec = codec;
  }

  void flushWriteBehindQueue(Chain ignored, long hash) {
    executorService.submit(() -> {
      try {
        Chain chain = clusteredWriteBehindStore.lock(hash);
        try {
          if (!chain.isEmpty()) {
            Map<K, PutOperation<K, V>> currentState = new HashMap<>();
            for (Element element : chain) {
              ByteBuffer payload = element.getPayload();
              Operation<K, V> operation = codec.decode(payload);
              K key = operation.getKey();
              PutOperation<K, V> result = resolver.applyOperation(key,
                                                                  currentState.get(key),
                                                                  operation);
              try {
                if (result != null) {
                  if (result != currentState.get(key) && !(operation instanceof PutOperation)) {
                    cacheLoaderWriter.write(result.getKey(), result.getValue());
                  }
                  currentState.put(key, result.asOperationExpiringAt(result.expirationTime()));
                } else {
                  if (currentState.get(key) != null && (operation instanceof RemoveOperation
                                                        || operation instanceof ConditionalRemoveOperation)) {
                    cacheLoaderWriter.delete(key);
                  }
                  currentState.remove(key);
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }

            ChainBuilder builder = new ChainBuilder();
            for (PutOperation<K, V> operation : currentState.values()) {
              builder = builder.add(codec.encode(operation));
            }

            clusteredWriteBehindStore.replaceAtHead(hash, chain, builder.build());
          }
        } finally {
          clusteredWriteBehindStore.unlock(hash, false);
        }
      } catch (TimeoutException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
