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

package org.ehcache.clustered.server.internal.messages;

import org.ehcache.clustered.common.internal.messages.EhcacheCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * EhcacheServerCodecTest
 */
public class EhcacheServerCodecTest {

  @Mock
  private EhcacheCodec clientCodec;

  @Mock
  private PassiveReplicationMessageCodec replicationCodec;

  private EhcacheServerCodec serverCodec;

  @Before
  public void setUp() {
    initMocks(this);
    serverCodec = new EhcacheServerCodec(clientCodec, replicationCodec);
  }

  @Test
  public void testDelegatesToEhcacheCodeForEncoding() throws Exception {
    LifecycleMessage lifecycleMessage = new LifecycleMessage() {

      private static final long serialVersionUID = 1L;

      @Override
      public EhcacheMessageType getMessageType() {
        return EhcacheMessageType.APPEND;
      }
    };
    serverCodec.encodeMessage(lifecycleMessage);

    verify(clientCodec).encodeMessage(any(EhcacheEntityMessage.class));
    verifyZeroInteractions(replicationCodec);
  }

  @Test
  public void testDelegatesToPassiveReplicationCodeForEncoding() throws Exception {
    InvalidationCompleteMessage message = new InvalidationCompleteMessage(1000L);
    serverCodec.encodeMessage(message);

    verify(replicationCodec).encode(message);
    verifyZeroInteractions(clientCodec);
  }

  @Test
  public void decodeLifeCycleMessages() throws Exception {
    for (EhcacheMessageType messageType : EhcacheMessageType.LIFECYCLE_MESSAGES) {
      ByteBuffer encodedBuffer = EhcacheCodec.OP_CODE_DECODER.encoder().enm("opCode", messageType).encode();
      serverCodec.decodeMessage(encodedBuffer.array());
    }
    verify(clientCodec, times(EhcacheMessageType.LIFECYCLE_MESSAGES.size())).decodeMessage(any(ByteBuffer.class), any(EhcacheMessageType.class));
    verifyZeroInteractions(replicationCodec);
  }

  @Test
  public void decodeServerStoreMessages() throws Exception {
    for (EhcacheMessageType messageType : EhcacheMessageType.STORE_OPERATION_MESSAGES) {
      ByteBuffer encodedBuffer = EhcacheCodec.OP_CODE_DECODER.encoder().enm("opCode", messageType).encode();
      serverCodec.decodeMessage(encodedBuffer.array());
    }
    verify(clientCodec, times(EhcacheMessageType.STORE_OPERATION_MESSAGES.size())).decodeMessage(any(ByteBuffer.class), any(EhcacheMessageType.class));
    verifyZeroInteractions(replicationCodec);
  }

  @Test
  public void decodeStateRepoMessages() throws Exception {
    for (EhcacheMessageType messageType : EhcacheMessageType.STATE_REPO_OPERATION_MESSAGES) {
      ByteBuffer encodedBuffer = EhcacheCodec.OP_CODE_DECODER.encoder().enm("opCode", messageType).encode();
      serverCodec.decodeMessage(encodedBuffer.array());
    }
    verify(clientCodec, times(EhcacheMessageType.STATE_REPO_OPERATION_MESSAGES.size())).decodeMessage(any(ByteBuffer.class), any(EhcacheMessageType.class));
    verifyZeroInteractions(replicationCodec);
  }

  @Test
  public void decodeClientIDTrackerMessages() throws Exception {
    for (EhcacheMessageType messageType : EhcacheMessageType.PASSIVE_REPLICATION_MESSAGES) {
      ByteBuffer encodedBuffer = EhcacheCodec.OP_CODE_DECODER.encoder().enm("opCode", messageType).encode();
      serverCodec.decodeMessage(encodedBuffer.array());
    }
    verify(replicationCodec, times(EhcacheMessageType.PASSIVE_REPLICATION_MESSAGES.size())).decode(any(EhcacheMessageType.class), any(ByteBuffer.class));
    verifyZeroInteractions(clientCodec);
  }
}
