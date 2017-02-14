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

import org.ehcache.clustered.client.internal.SimpleClusterTierManagerClientEntity;
import org.ehcache.clustered.client.internal.config.ExperimentalClusteringServiceConfiguration;
import org.ehcache.config.Builder;

import java.util.concurrent.TimeUnit;

/**
 * Extends {@link ClusteringServiceConfiguration} to add timeouts for
 * {@link SimpleClusterTierManagerClientEntity ClusterTierManagerClientEntity}
 * operation timeouts.
 */
public class TestClusteringServiceConfiguration extends ClusteringServiceConfiguration
    implements ExperimentalClusteringServiceConfiguration {

  private final TimeoutDuration mutativeOperationTimeout;
  private final TimeoutDuration lifecycleOperationTimeout;

  public static TestClusteringServiceConfiguration of(ClusteringServiceConfiguration baseConfig) {
    return new TestClusteringServiceConfiguration(baseConfig, null, null);
  }

  public static TestClusteringServiceConfiguration of(Builder<ClusteringServiceConfiguration> baseBuilder) {
    return new TestClusteringServiceConfiguration(baseBuilder.build(), null, null);
  }

  private TestClusteringServiceConfiguration(ClusteringServiceConfiguration baseConfig, TimeoutDuration mutativeOperationTimeout, TimeoutDuration lifecycleOperationTimeout) {
    super(baseConfig);
    this.mutativeOperationTimeout = mutativeOperationTimeout;
    this.lifecycleOperationTimeout = lifecycleOperationTimeout;
  }

  public TestClusteringServiceConfiguration mutativeOperationTimeout(long amount, TimeUnit unit) {
    return new TestClusteringServiceConfiguration(this, TimeoutDuration.of(amount, unit), this.lifecycleOperationTimeout);
  }

  public TestClusteringServiceConfiguration lifecycleOperationTimeout(long amount, TimeUnit unit) {
    return new TestClusteringServiceConfiguration(this, this.mutativeOperationTimeout, TimeoutDuration.of(amount, unit));
  }

  @Override
  public TimeoutDuration getMutativeOperationTimeout() {
    return mutativeOperationTimeout;
  }

  @Override
  public TimeoutDuration getLifecycleOperationTimeout() {
    return lifecycleOperationTimeout;
  }
}
