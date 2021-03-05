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

package org.ehcache.clustered.common.internal.store.operations;

import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.EternalChainResolver;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.expiry.ExpiryPolicy;

import static org.ehcache.config.builders.ExpiryPolicyBuilder.noExpiration;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

public class EternalChainResolverTest extends AbstractChainResolverTest {

  @Override
  protected ChainResolver<Long, String> createChainResolver(ExpiryPolicy<? super Long, ? super String> expiryPolicy, OperationsCodec<Long, String> codec) {
    assumeThat(expiryPolicy, is(noExpiration()));
    return new EternalChainResolver<>(codec);
  }
}
