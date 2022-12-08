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
package org.ehcache.core.statistics;

import java.util.EnumSet;

public class Jsr107LatencyMonitor<T extends Enum<T>> implements ChainedOperationObserver<T> {

  private final org.terracotta.statistics.derived.latency.Jsr107LatencyMonitor<T> delegate;

  public Jsr107LatencyMonitor(Class<T> outcome) {
    delegate = new org.terracotta.statistics.derived.latency.Jsr107LatencyMonitor<>(EnumSet.allOf(outcome), 1.0);
  }

  public double average() {
    return delegate.average();
  }

  public void clear() {
    delegate.clear();
  }

  @Override
  public void begin(long time) {
    delegate.begin(time);
  }

  @Override
  public void end(long time, long latency, T result) {
    delegate.end(time, latency, result);
  }
}
