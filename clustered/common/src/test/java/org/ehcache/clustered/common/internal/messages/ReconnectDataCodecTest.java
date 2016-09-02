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

package org.ehcache.clustered.common.internal.messages;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class ReconnectDataCodecTest {

  @Test
  public void testCodec() {
    Set<String> cacheIds = new HashSet<String>();
    cacheIds.add("test");
    cacheIds.add("test1");
    cacheIds.add("test2");

    ReconnectDataCodec dataCodec = new ReconnectDataCodec();

    Set<String> decoded = dataCodec.decode(dataCodec.encode(cacheIds, 14));

    assertThat(decoded, Matchers.hasSize(3));
    assertThat(decoded, containsInAnyOrder("test", "test1", "test2"));


  }
}
