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

import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.Builder;

/**
 * {@link Builder} for the {@link ClusteredStoreConfiguration}.
 */
public class ClusteredStoreConfigurationBuilder implements Builder<ClusteredStoreConfiguration> {

  private final Consistency consistency;

  /**
   * Creates a new builder instance with the provided {@link Consistency} configured.
   *
   * @param consistency the {@code Consistency}
   * @return a {@code Builder} instance
   */
  public static ClusteredStoreConfigurationBuilder withConsistency(Consistency consistency) {
    return new ClusteredStoreConfigurationBuilder(consistency);
  }

  ClusteredStoreConfigurationBuilder(Consistency consistency) {
    this.consistency = consistency;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClusteredStoreConfiguration build() {
    return new ClusteredStoreConfiguration(consistency);
  }
}
