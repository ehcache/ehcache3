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
package org.ehcache.clustered.client.config.builders;

import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.config.Builder;

import java.time.Duration;
import java.util.Objects;

/**
 * Constructs instances of {@link Timeouts}. Initially, all timeouts are set to their default.
 */
public final class TimeoutsBuilder implements Builder<Timeouts> {
  private Duration readOperationTimeout = Timeouts.DEFAULT.getReadOperationTimeout();
  private Duration writeOperationTimeout = Timeouts.DEFAULT.getWriteOperationTimeout();
  private Duration connectionTimeout = Timeouts.DEFAULT.getConnectionTimeout();

  /**
   * Obtain a new get for timeouts
   *
   * @return timeout get
   */
  public static TimeoutsBuilder timeouts() {
    return new TimeoutsBuilder();
  }

  private TimeoutsBuilder() {}

  /**
   * Sets the timeout for read operations.  The default value for this timeout is
   * {@link Timeouts#DEFAULT_OPERATION_TIMEOUT}.
   *
   * @param readOperationTimeout the {@code Duration} to use for the read operation timeout
   *
   * @return this {@code Builder}
   */
  public TimeoutsBuilder read(Duration readOperationTimeout) {
    this.readOperationTimeout = Objects.requireNonNull(readOperationTimeout, "Read operation timeout can't be null");
    return this;
  }

  /**
   * Sets the timeout for write operations like {@code put} and {@code remove}. The default value for this timeout
   * is {@link Timeouts#DEFAULT_OPERATION_TIMEOUT}.
   *
   * @param writeOperationTimeout the {@code Duration} to use for a write operation timeout
   *
   * @return this {@code Builder}
   */
  public TimeoutsBuilder write(Duration writeOperationTimeout) {
    this.writeOperationTimeout = Objects.requireNonNull(writeOperationTimeout, "Write operation timeout can't be null");
    return this;
  }

  /**
   * Sets the timeout for connecting to the server. The default value for this timeout
   * is {@link Timeouts#INFINITE_TIMEOUT}.
   *
   * @param connectionTimeout the {@code Duration} to use for a connection timeout
   *
   * @return this {@code Builder}
   */
  public TimeoutsBuilder connection(Duration connectionTimeout) {
    this.connectionTimeout = Objects.requireNonNull(connectionTimeout, "Connection timeout can't be null");
    return this;
  }

  /**
   * Gets a new {@link Timeouts} instance using the current timeout duration settings.
   *
   * @return a new {@code Timeouts} instance
   */
  public Timeouts build() {
    return new Timeouts(readOperationTimeout, writeOperationTimeout, connectionTimeout);
  }
}
