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

package org.ehcache.core;

import org.ehcache.Status;
import org.ehcache.core.spi.store.Store;
import org.ehcache.StateTransitionException;
import org.ehcache.core.spi.LifeCycled;
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public abstract class CacheTest {

  protected abstract InternalCache<Object, Object> getCache(Store<Object, Object> store);

  @Test
  public void testTransistionsState() {
    Store<Object, Object> store = mock(Store.class);

    InternalCache ehcache = getCache(store);
    assertThat(ehcache.getStatus(), CoreMatchers.is(Status.UNINITIALIZED));
    ehcache.init();
    assertThat(ehcache.getStatus(), is(Status.AVAILABLE));
    ehcache.close();
    assertThat(ehcache.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testThrowsWhenNotAvailable() throws StoreAccessException {
    Store<Object, Object> store = mock(Store.class);
    Store.Iterator mockIterator = mock(Store.Iterator.class);
    when(store.iterator()).thenReturn(mockIterator);
    InternalCache<Object, Object> ehcache = getCache(store);

    try {
      ehcache.get("foo");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.put("foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.remove("foo");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.remove("foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.containsKey("foo");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.replace("foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.replace("foo", "foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.putIfAbsent("foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.clear();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.iterator();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.getAll(Collections.singleton("foo"));
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.removeAll(Collections.singleton("foo"));
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.putAll(Collections.singletonMap("foo", "bar"));
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    ehcache.init();
    final Iterator iterator = ehcache.iterator();
    ehcache.close();
    try {
      iterator.hasNext();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
    try {
      iterator.next();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
    try {
      iterator.remove();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
  }

  @Test
  public void testDelegatesLifecycleCallsToStore() throws Exception {
    InternalCache ehcache = getCache(mock(Store.class));
    final LifeCycled mock = mock(LifeCycled.class);
    ehcache.addHook(mock);
    ehcache.init();
    verify(mock).init();
    ehcache.close();
    verify(mock).close();
  }

  @Test
  public void testFailingTransitionGoesToLowestStatus() throws Exception {
    final LifeCycled mock = mock(LifeCycled.class);
    InternalCache ehcache = getCache(mock(Store.class));
    doThrow(new Exception()).when(mock).init();
    ehcache.addHook(mock);
    try {
      ehcache.init();
      fail();
    } catch (StateTransitionException e) {
      assertThat(ehcache.getStatus(), is(Status.UNINITIALIZED));
    }

    reset(mock);
    ehcache.init();
    assertThat(ehcache.getStatus(), is(Status.AVAILABLE));
    ehcache.close();
  }

  @Test
  public void testPutIfAbsent() throws StoreAccessException {
    final AtomicReference<Object> existingValue = new AtomicReference<>();
    final Store store = mock(Store.class);
    final String value = "bar";
    when(store.computeIfAbsent(eq("foo"), any(Function.class))).thenAnswer(invocationOnMock -> {
      final Function<Object, Object> biFunction
          = (Function<Object, Object>)invocationOnMock.getArguments()[1];
      if (existingValue.get() == null) {
        final Object newValue = biFunction.apply(invocationOnMock.getArguments()[0]);
        existingValue.compareAndSet(null, newValue);
      }
      return new Store.ValueHolder<Object>() {
        @Override
        public Object get() {
          return existingValue.get();
        }

        @Override
        public long creationTime() {
          throw new UnsupportedOperationException("Implement me!");
        }

        @Override
        public long expirationTime() {
          throw new UnsupportedOperationException("Implement me!");
        }

        @Override
        public boolean isExpired(long expirationTime) {
          throw new UnsupportedOperationException("Implement me!");
        }

        @Override
        public long lastAccessTime() {
          throw new UnsupportedOperationException("Implement me!");
        }

        @Override
        public long getId() {
          throw new UnsupportedOperationException("Implement me!");
        }
      };
    });
    when(store.putIfAbsent(eq("foo"), any(String.class), any(Consumer.class))).then(invocation -> {
      final Object toReturn;
      if ((toReturn = existingValue.get()) == null) {
        existingValue.compareAndSet(null, invocation.getArguments()[1]);
      }
      return new Store.ValueHolder<Object>() {
        @Override
        public Object get() {
          return toReturn;
        }

        @Override
        public long creationTime() {
          throw new UnsupportedOperationException("Implement me!");
        }

        @Override
        public long expirationTime() {
          throw new UnsupportedOperationException("Implement me!");
        }

        @Override
        public boolean isExpired(long expirationTime) {
          throw new UnsupportedOperationException("Implement me!");
        }

        @Override
        public long lastAccessTime() {
          throw new UnsupportedOperationException("Implement me!");
        }

        @Override
        public long getId() {
          throw new UnsupportedOperationException("Implement me!");
        }
      };
    });
    InternalCache<Object, Object> ehcache = getCache(store);
    ehcache.init();
    assertThat(ehcache.putIfAbsent("foo", value), nullValue());
    assertThat(ehcache.putIfAbsent("foo", "foo"), CoreMatchers.is(value));
    assertThat(ehcache.putIfAbsent("foo", "foobar"), CoreMatchers.is(value));
    assertThat(ehcache.putIfAbsent("foo", value), CoreMatchers.is(value));
  }

  @Test
  public void testInvokesHooks() {
    Store store = mock(Store.class);
    InternalCache ehcache = getCache(store);

    final LifeCycled hook = mock(LifeCycled.class);
    ehcache.addHook(hook);
    ehcache.init();
    try {
      verify(hook).init();
    } catch (Exception e) {
      fail();
    }
    reset(hook);
    try {
      if (ehcache instanceof Ehcache) {
        ((Ehcache)ehcache).removeHook(hook);
      } else {
        ((Ehcache)ehcache).removeHook(hook);
      }
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
    ehcache.close();
    try {
      verify(hook).close();
    } catch (Exception e) {
      fail();
    }
  }

}
