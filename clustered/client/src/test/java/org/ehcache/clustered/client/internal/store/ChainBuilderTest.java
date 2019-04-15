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
package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.util.ChainBuilder;
import org.junit.Test;

import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.Matchers.hasPayloads;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 */
public class ChainBuilderTest {

  @Test
  public void testChainBuilder() {
    Chain chain = new ChainBuilder()
      .add(createPayload(1L))
      .add(createPayload(3L))
      .add(createPayload(4L))
      .add(createPayload(2L)).build();

    assertThat(chain, hasPayloads(1L, 3L, 4L, 2L));
  }
}
