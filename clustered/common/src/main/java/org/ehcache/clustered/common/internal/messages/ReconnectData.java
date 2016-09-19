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

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ReconnectData {

  private static final byte CLIENT_ID_SIZE = 16;
  private static final byte ENTRY_SIZE = 4;

  private volatile UUID clientId;
  private final Set<String> reconnectData = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final AtomicInteger reconnectDatalen = new AtomicInteger(CLIENT_ID_SIZE);

  public UUID getClientId() {
    if (clientId == null) {
      throw new AssertionError("Client ID cannot be null");
    }
    return clientId;
  }

  public void setClientId(UUID clientId) {
    this.clientId = clientId;
  }

  public void add(String name) {
    reconnectData.add(name);
    reconnectDatalen.addAndGet(2 * name.length() + ENTRY_SIZE);
  }

  public void remove(String name) {
    if (!reconnectData.contains(name)) {
      reconnectData.remove(name);
      reconnectDatalen.addAndGet(-(2 * name.length() + ENTRY_SIZE));
    }
  }

  public Set<String> getAllCaches() {
    return Collections.unmodifiableSet(reconnectData);
  }

  public int getDataLength() {
    return reconnectDatalen.get();
  }

}
