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

import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * {@link ServiceConfiguration} for the {@link ClusteredStore}.
 */
public class ClusteredStoreConfiguration implements ServiceConfiguration<ClusteredStore.Provider, Consistency> {

  private final Consistency consistency;

  /**
   * Creates a new configuration with consistency set to {@link Consistency#EVENTUAL EVENTUAL}.
   */
  public ClusteredStoreConfiguration() {
    this(Consistency.EVENTUAL);
  }

  /**
   * Creates a new configuration with the provided {@link Consistency}.
   *
   * @param consistency the {@code Consistency}
   */
  public ClusteredStoreConfiguration(Consistency consistency) {
    this.consistency = consistency;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<ClusteredStore.Provider> getServiceType() {
    return ClusteredStore.Provider.class;
  }

  /**
   * Returns the {@link Consistency} for this configuration instance.
   *
   * @return the {@code Consistency}
   */
  public Consistency getConsistency() {
    return consistency;
  }

  @Override
  public Consistency derive() {
    return getConsistency();
  }

  @Override
  public ClusteredStoreConfiguration build(Consistency representation) {
    return new ClusteredStoreConfiguration(representation);
  }
}
