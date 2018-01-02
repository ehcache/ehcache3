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

package org.ehcache.clustered.lock.server.messages;

import org.ehcache.clustered.common.internal.lock.LockMessaging;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.SyncMessageCodec;

/**
 * LockSyncMessaging
 */
public class LockSyncMessaging {

  public static SyncMessageCodec<LockMessaging.LockOperation> syncCodec() {
    return SYNC_CODEC;
  }

  private static final SyncMessageCodec<LockMessaging.LockOperation> SYNC_CODEC = new SyncMessageCodec<LockMessaging.LockOperation>() {
    @Override
    public byte[] encode(int i, LockMessaging.LockOperation message) {
      throw new AssertionError();
    }

    @Override
    public LockMessaging.LockOperation decode(int i, byte[] bytes) {
      throw new AssertionError();
    }
  };

}
