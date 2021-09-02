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

package org.ehcache.clustered.server.repo;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class StateRepositoryManagerTest {

  @Test
  public void testInvokeOnNonExistentRepositorySucceeds() throws Exception {
    StateRepositoryManager manager = new StateRepositoryManager();
    EhcacheEntityResponse.MapValue response = (EhcacheEntityResponse.MapValue) manager.invoke(
        new StateRepositoryOpMessage.PutIfAbsentMessage("foo", "bar", "key1", "value1"));
    assertThat(response.getValue(), nullValue());
    response = (EhcacheEntityResponse.MapValue) manager.invoke(
        new StateRepositoryOpMessage.GetMessage("foo", "bar", "key1"));
    assertThat(response.getValue(), is("value1"));
  }
}
