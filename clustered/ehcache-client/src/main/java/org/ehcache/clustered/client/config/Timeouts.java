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
package org.ehcache.clustered.client.config;

import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;

import java.time.Duration;

import java.util.function.LongSupplier;

/**
 * Describes the timeouts for {@link ClusterTierManagerClientEntity} operations.  Use
 * {@link TimeoutsBuilder} to construct an instance.
 */
public final class Timeouts {

  public static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(150);
  public static final Duration INFINITE_TIMEOUT = Duration.ofNanos(Long.MAX_VALUE);
  public static final Timeouts DEFAULT = new Timeouts(DEFAULT_OPERATION_TIMEOUT, DEFAULT_OPERATION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);

  private final Duration readOperationTimeout;
  private final Duration writeOperationTimeout;
  private final Duration connectionTimeout;

  public Timeouts(Duration readOperationTimeout, Duration writeOperationTimeout, Duration connectionTimeout) {
    this.readOperationTimeout = neverBeAfterInfinite(readOperationTimeout);
    this.writeOperationTimeout = neverBeAfterInfinite(writeOperationTimeout);
    this.connectionTimeout = neverBeAfterInfinite(connectionTimeout);
  }

  private Duration neverBeAfterInfinite(Duration duration) {
    return (duration.compareTo(INFINITE_TIMEOUT) > 0) ? INFINITE_TIMEOUT : duration;
  }

  public Duration getReadOperationTimeout() {
    return readOperationTimeout;
  }

  public Duration getWriteOperationTimeout() {
    return writeOperationTimeout;
  }

  public Duration getConnectionTimeout() {
    return connectionTimeout;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Timeouts timeouts = (Timeouts) o;

    if (!readOperationTimeout.equals(timeouts.readOperationTimeout)) {
      return false;
    }
    if (!writeOperationTimeout.equals(timeouts.writeOperationTimeout)) {
      return false;
    }
    return connectionTimeout.equals(timeouts.connectionTimeout);
  }

  @Override
  public int hashCode() {
    int result = readOperationTimeout.hashCode();
    result = 31 * result + writeOperationTimeout.hashCode();
    result = 31 * result + connectionTimeout.hashCode();
    return result;
  }

  public static LongSupplier nanosStartingFromNow(Duration timeout) {
    long end = System.nanoTime() + timeout.toNanos();
    return () -> end - System.nanoTime();
  }

  @Override
  public String toString() {
    return "Timeouts{" +
           "readOperation=" + readOperationTimeout +
           ", writeOperation=" + writeOperationTimeout +
           ", connection=" + connectionTimeout +
           '}';
  }
}
