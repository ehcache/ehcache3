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

import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.event.EventType;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.ehcache.impl.internal.store.offheap.AbstractOffHeapStoreTest.eventType;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * FudgingInvocationScopedEventSinkTest
 */
public class FudgingInvocationScopedEventSinkTest {

  private StoreEventListener<String, String> listener;
  private FudgingInvocationScopedEventSink<String, String> eventSink;
  private Matcher<StoreEvent<String, String>> createdMatcher = eventType(EventType.CREATED);
  private Matcher<StoreEvent<String, String>> evictedMatcher = eventType(EventType.EVICTED);

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    HashSet<StoreEventListener<String, String>> storeEventListeners = new HashSet<>();
    listener = mock(StoreEventListener.class);
    storeEventListeners.add(listener);
    eventSink = new FudgingInvocationScopedEventSink<String, String>(new HashSet<>(),
        false, new BlockingQueue[] { new ArrayBlockingQueue<FireableStoreEventHolder<String, String>>(10) }, storeEventListeners);

  }

  @Test
  public void testEvictedDifferentKeyNoImpact() {
    eventSink.created("k1", "v1");
    eventSink.evicted("k2", () -> "v2");
    eventSink.close();

    InOrder inOrder = inOrder(listener);
    inOrder.verify(listener).onEvent(argThat(createdMatcher));
    inOrder.verify(listener).onEvent(argThat(evictedMatcher));
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testEvictedSameKeyAfterUpdateReplacesWithEvictCreate() {
    eventSink.updated("k1", () -> "v0", "v1");
    eventSink.evicted("k1", () -> "v0");
    eventSink.close();

    InOrder inOrder = inOrder(listener);
    inOrder.verify(listener).onEvent(argThat(evictedMatcher));
    inOrder.verify(listener).onEvent(argThat(createdMatcher));
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testEvictedSameKeyAfterCreateFudgesExpiryToo() {
    eventSink.expired("k1", () -> "v0");
    eventSink.created("k1", "v1");
    eventSink.evicted("k1", () -> "v0");
    eventSink.close();

    InOrder inOrder = inOrder(listener);
    inOrder.verify(listener).onEvent(argThat(evictedMatcher));
    inOrder.verify(listener).onEvent(argThat(createdMatcher));
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testEvictedSameKeyAfterUpdateReplacesWithEvictCreateEvenWithMultipleEvictsInBetween() {
    eventSink.updated("k1", () -> "v0", "v1");
    eventSink.evicted("k2", () -> "v2");
    eventSink.evicted("k3", () -> "v3");
    eventSink.evicted("k1", () -> "v0");
    eventSink.close();

    InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(3)).onEvent(argThat(evictedMatcher));
    inOrder.verify(listener).onEvent(argThat(createdMatcher));
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testEvictedSameKeyAfterCreateFudgesExpiryTooEvenWithMultipleEvictsInBetween() {
    eventSink.expired("k1", () -> "v0");
    eventSink.created("k1", "v1");
    eventSink.evicted("k2", () -> "v2");
    eventSink.evicted("k3", () -> "v3");
    eventSink.evicted("k1", () -> "v0");
    eventSink.close();

    InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(3)).onEvent(argThat(evictedMatcher));
    inOrder.verify(listener).onEvent(argThat(createdMatcher));
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testEvictedKeyDoesNotFudgeOlderEvents() {
    eventSink.updated("k1", () -> "v0", "v1");
    eventSink.created("k2", "v2");
    eventSink.evicted("k1", () -> "v0");
    eventSink.close();

    InOrder inOrder = inOrder(listener);
    Matcher<StoreEvent<String, String>> updatedMatcher = eventType(EventType.UPDATED);
    inOrder.verify(listener).onEvent(argThat(updatedMatcher));
    inOrder.verify(listener).onEvent(argThat(createdMatcher));
    inOrder.verify(listener).onEvent(argThat(evictedMatcher));
    verifyNoMoreInteractions(listener);
  }
}
