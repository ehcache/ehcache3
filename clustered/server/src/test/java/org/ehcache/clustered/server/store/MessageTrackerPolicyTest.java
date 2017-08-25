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
package org.ehcache.clustered.server.store;

import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

public class MessageTrackerPolicyTest {

  private static class TypedOperationMessage extends EhcacheOperationMessage {

    private final EhcacheMessageType type;

    public TypedOperationMessage(EhcacheMessageType type) {
      this.type = type;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return type;
    }

    @Override
    public void setId(long id) {

    }

    @Override
    public long getId() {
      return 0;
    }

    @Override
    public UUID getClientId() {
      return null;
    }
  }

  private MessageTrackerPolicy policy = new MessageTrackerPolicy(EhcacheMessageType.REPLACE::equals);

  @Test
  public void trackable_wrongMessageType() throws Exception {
    assertThat(policy.trackable(new InvalidMessage())).isFalse();
  }

  @Test
  public void trackable_mutableType() throws Exception {
    assertThat(policy.trackable(new TypedOperationMessage(EhcacheMessageType.REPLACE))).isTrue();
  }

  @Test
  public void trackable_nonmutableType() throws Exception {
    assertThat(policy.trackable(new TypedOperationMessage(EhcacheMessageType.GET_STORE))).isFalse();
  }
}
