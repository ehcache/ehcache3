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
package org.ehcache.impl.internal.jctools;

import org.ehcache.impl.internal.concurrent.EvictingConcurrentMap;
import org.ehcache.impl.internal.concurrent.EvictingConcurrentMapTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.*;
import static org.ehcache.config.Eviction.noAdvice;

public class NonBlockingHashMapTest extends EvictingConcurrentMapTest {

  @Override
  protected <K, V> EvictingConcurrentMap<K, V> initMap() {
    return new NonBlockingHashMap<>();
  }

  @Override
  protected <K, V> EvictingConcurrentMap<K, V> initMap(int size) {
    return new NonBlockingHashMap<>(size);
  }
}
