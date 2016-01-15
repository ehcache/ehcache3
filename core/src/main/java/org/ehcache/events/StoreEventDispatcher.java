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

package org.ehcache.events;

import org.ehcache.spi.cache.events.StoreEventSource;

/**
 * Part of the events subsystem at the {@link org.ehcache.spi.cache.Store} level.
 * <P/>
 * This interface controls the lifecycle of {@link StoreEventSink}s, enabling implementations to decouple the event
 * raising inside the {@link org.ehcache.spi.cache.Store} from the firing to outside collaborators.
 * <P/>
 * {@link org.ehcache.spi.cache.Store} implementations are expected to get a {@link StoreEventSink} per
 * operation and release it once the operation completes.
 */
public interface StoreEventDispatcher<K, V> extends StoreEventSource<K, V> {

  StoreEventSink<K, V> eventSink();

  void releaseEventSink(StoreEventSink<K, V> eventSink);

  void releaseEventSinkAfterFailure(StoreEventSink<K, V> eventSink, Throwable throwable);
}
