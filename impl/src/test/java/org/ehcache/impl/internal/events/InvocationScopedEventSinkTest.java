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

package org.ehcache.impl.internal.events;

import org.ehcache.core.spi.store.events.StoreEventFilter;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.event.EventType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.ehcache.core.internal.util.ValueSuppliers.supplierOf;
import static org.ehcache.impl.internal.store.offheap.AbstractOffHeapStoreTest.eventType;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * InvocationScopedEventSinkTest
 */
public class InvocationScopedEventSinkTest {

  private StoreEventListener listener;
  private InvocationScopedEventSink<String, String> eventSink;

  @Before
  public void setUp() {
    HashSet<StoreEventListener<String, String>> storeEventListeners = new HashSet<StoreEventListener<String, String>>();
    listener = mock(StoreEventListener.class);
    storeEventListeners.add(listener);
    eventSink = new InvocationScopedEventSink<String, String>(new HashSet<StoreEventFilter<String, String>>(),
        false, new BlockingQueue[] { new ArrayBlockingQueue<FireableStoreEventHolder<String, String>>(10) }, storeEventListeners);

  }

  @Test
  public void testReset() {
    eventSink.created("k1", "v1");
    eventSink.evicted("k1", supplierOf("v2"));
    eventSink.reset();
    eventSink.created("k1", "v1");
    eventSink.updated("k1", supplierOf("v1"), "v2");
    eventSink.evicted("k1", supplierOf("v2"));
    eventSink.close();

    InOrder inOrder = inOrder(listener);
    inOrder.verify(listener).onEvent(argThat(eventType(EventType.CREATED)));
    inOrder.verify(listener).onEvent(argThat(eventType(EventType.UPDATED)));
    inOrder.verify(listener).onEvent(argThat(eventType(EventType.EVICTED)));
    verifyNoMoreInteractions(listener);
  }

}