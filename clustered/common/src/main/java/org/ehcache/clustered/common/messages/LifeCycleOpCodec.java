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
package org.ehcache.clustered.common.messages;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public class LifeCycleOpCodec {

  public static byte[] encode(LifecycleMessage message) {
    byte[] encodedMsg = marshall(message);
    ByteBuffer buffer = ByteBuffer.allocate(1 + encodedMsg.length);
    buffer.put(EhcacheEntityMessage.Type.LIFECYCLE_OP.getOpCode());
    buffer.put(encodedMsg);
    return buffer.array();
  }

  public static LifecycleMessage decode(ByteBuffer payload) {
    byte[] payArr = new byte[payload.remaining()];
    payload.get(payArr);
    return (LifecycleMessage) unmarshall(payArr);
  }

  public static Object unmarshall(byte[] payload) {
    try {
      return new ObjectInputStream(new ByteArrayInputStream(payload)).readObject();
    } catch (IOException ex) {
      throw new IllegalArgumentException(ex);
    } catch (ClassNotFoundException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  public static byte[] marshall(Object message) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oout = new ObjectOutputStream(out);
      try {
        oout.writeObject(message);
      } finally {
        oout.close();
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    return out.toByteArray();
  }
}
