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
package org.ehcache.util;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Similar to jQuery $.Deferred: can only be resolved once, and once resolved, will call all {@link org.ehcache.util.Deferred.Consumer}.
 * When resolved, consumers are called directly.
 *
 * @author Mathieu Carbou
 */
public class Deferred<V> {

  private volatile V obj;
  private boolean resolved;
  private final Queue<Consumer<V>> dones = new LinkedList<Consumer<V>>();
  private final CountDownLatch latch = new CountDownLatch(1);

  public Deferred() {
  }

  public Deferred(V obj) {
    resolve(obj);
  }

  public synchronized Deferred<V> resolve(V obj) throws IllegalStateException {
    if (this.resolved) {
      throw new IllegalStateException("Already resolved");
    }
    this.obj = obj;
    this.resolved = true;
    latch.countDown();
    while (!dones.isEmpty()) dones.poll().consume(obj);
    return this;
  }

  public synchronized Deferred<V> done(Consumer<V> consumer) {
    if (resolved) consumer.consume(obj);
    else dones.add(consumer);
    return this;
  }

  public V get() throws InterruptedException {
    latch.await();
    return obj;
  }

  public V get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
    if (latch.await(timeout, unit)) {
      return obj;
    } else {
      throw new TimeoutException("After " + timeout + " " + unit);
    }
  }

  public static <T> Deferred<T> create() {
    return new Deferred<T>();
  }

  public static <T> Deferred<T> of(T resolved) {
    return new Deferred<T>(resolved);
  }

  public interface Consumer<T> {
    void consume(T obj);
  }
}
