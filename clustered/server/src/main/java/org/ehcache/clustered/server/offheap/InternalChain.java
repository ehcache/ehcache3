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

import java.io.Closeable;
import java.nio.ByteBuffer;

import org.ehcache.clustered.common.internal.store.Chain;

public interface InternalChain extends Closeable {

  Chain detach();

  boolean append(ByteBuffer element);

  ReplaceResponse replace(Chain expected, Chain replacement);

  @Override
  void close();

  enum ReplaceResponse {
    /**
     * Denotes head of the current chain matches the expected chain and it was replaced with replacement chain
     */
    MATCH_AND_REPLACED,

    /**
     * Denotes head of the current chain matches the expected chain and it was not replaced with replacement chain
     */
    MATCH_BUT_NOT_REPLACED,

    /**
     * Denotes current chain matches the expected chain and it was replaced with replacement chain
     */
    EXACT_MATCH_AND_REPLACED,

    /**
     * Denotes head of the current chain doesn't match the expected chain
     */
    NO_MATCH
  }
}
