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

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EhcacheEntityMessageTest {

  @Test
  public void testEhcacheEntityMessageTypes() {
    assertThat(EhcacheEntityMessage.Type.LIFECYCLE_OP.getCode(), is((byte) 10));
    assertThat(EhcacheEntityMessage.Type.SERVER_STORE_OP.getCode(), is((byte) 20));
    assertThat(EhcacheEntityMessage.Type.STATE_REPO_OP.getCode(), is((byte) 30));
    assertThat(EhcacheEntityMessage.Type.SYNC_OP.getCode(), is((byte) 40));
    assertThat(EhcacheEntityMessage.Type.REPLICATION_OP.getCode(), is((byte) 50));
  }

}
