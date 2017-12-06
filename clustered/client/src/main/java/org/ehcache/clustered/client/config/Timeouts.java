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

import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;

import java.time.Duration;
import java.util.Objects;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.SECONDS;
import java.util.function.LongSupplier;

/**
 * Describes the timeouts for {@link ClusterTierManagerClientEntity} operations.  Use
 * {@link #builder()} to construct an instance.
 */
public final class Timeouts {

  public static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration INFINITE_TIMEOUT = Duration.ofMillis(Long.MAX_VALUE);

  private final Duration readOperationTimeout;
  private final Duration mutativeOperationTimeout;
  private final Duration connectionTimeout;

  private Timeouts(Duration readOperationTimeout, Duration mutativeOperationTimeout, Duration connectionTimeout) {
    this.readOperationTimeout = readOperationTimeout;
    this.mutativeOperationTimeout = mutativeOperationTimeout;
    this.connectionTimeout = connectionTimeout;
  }

  public Duration getReadOperationTimeout() {
    return readOperationTimeout;
  }

  public Duration getMutativeOperationTimeout() {
    return mutativeOperationTimeout;
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
    if (!mutativeOperationTimeout.equals(timeouts.mutativeOperationTimeout)) {
      return false;
    }
    return connectionTimeout.equals(timeouts.connectionTimeout);
  }

  @Override
  public int hashCode() {
    int result = readOperationTimeout.hashCode();
    result = 31 * result + mutativeOperationTimeout.hashCode();
    result = 31 * result + connectionTimeout.hashCode();
    return result;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static LongSupplier nanosStartingFromNow(Duration timeout) {
    long end = System.nanoTime() + timeout.toNanos();
    return () -> end - System.nanoTime();
  }

  @Override
  public String toString() {
    return "Timeouts{" +
        "readOperation=" + readOperationTimeout +
        ", mutativeOperation=" + mutativeOperationTimeout +
        ", connection=" + connectionTimeout +
        '}';
  }

  /**
   * Constructs instances of {@link Timeouts}.  When obtained from
   * {@link Timeouts#builder()}, the default values are pre-set.
   */
  public static final class Builder implements org.ehcache.config.Builder<Timeouts> {
    private Duration readOperationTimeout = DEFAULT_OPERATION_TIMEOUT;
    private Duration mutativeOperationTimeout = DEFAULT_OPERATION_TIMEOUT;
    private Duration connectionTimeout = INFINITE_TIMEOUT;

    /**
     * Sets the timeout for read operations.  The default value for this timeout is
     * 5 seconds.
     *
     * @param readOperationTimeout the {@code Duration} to use for the read operation timeout
     *
     * @return this {@code Builder}
     */
    public Builder setReadOperationTimeout(Duration readOperationTimeout) {
      this.readOperationTimeout = Objects.requireNonNull(readOperationTimeout, "Read operation timeout can't be null");
      return this;
    }

    /**
     * Sets the timeout for mutative operations like {@code put} and {@code remove}. The default value for this timeout
     * is 5 seconds.
     *
     * @param mutativeOperationTimeout the {@code Duration} to use for a mutative operation timeout
     *
     * @return this {@code Builder}
     */
    public Builder setMutativeOperationTimeout(Duration mutativeOperationTimeout) {
      this.mutativeOperationTimeout = Objects.requireNonNull(mutativeOperationTimeout, "Mutative operation timeout can't be null");
      return this;
    }

    /**
     * Sets the timeout for connecting to the server. The default value for this timeout
     * is {@link #INFINITE_TIMEOUT}.
     *
     * @param connectionTimeout the {@code Duration} to use for a connection timeout
     *
     * @return this {@code Builder}
     */
    public Builder setConnectionTimeout(Duration connectionTimeout) {
      this.connectionTimeout = Objects.requireNonNull(connectionTimeout, "Connection timeout can't be null");
      return this;
    }

    /**
     * Gets a new {@link Timeouts} instance using the current timeout duration settings.
     *
     * @return a new {@code Timeouts} instance
     */
    public Timeouts build() {
      return new Timeouts(readOperationTimeout, mutativeOperationTimeout, connectionTimeout);
    }
  }
}
