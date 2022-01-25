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

package org.ehcache.transactions.xa.internal;

import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.StatefulSerializer;

import java.util.concurrent.atomic.AtomicReference;

/**
 * StatefulSoftLockValueCombinedSerializer
 */
class StatefulSoftLockValueCombinedSerializer<T> extends SoftLockValueCombinedSerializer<T> implements StatefulSerializer<SoftLock<T>> {

  StatefulSoftLockValueCombinedSerializer(AtomicReference<? extends Serializer<SoftLock<T>>> softLockSerializerRef, Serializer<T> valueSerializer) {
    super(softLockSerializerRef, valueSerializer);
  }

  @Override
  public void init(StateRepository stateRepository) {
    ((StatefulSerializer<T>) valueSerializer).init(stateRepository);
  }
}
