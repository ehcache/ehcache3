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
package org.ehcache.clustered.messages;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.terracotta.entity.MessageCodec;

/**
 *
 * @author cdennis
 */
public class EhcacheCodec {
  
  private static final MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> SERVER_INSTANCE = new MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse>() {
    @Override
    public EhcacheEntityMessage deserialize(byte[] payload) {
      return deserializeMessage(payload);
    }

    @Override
    public EhcacheEntityMessage deserializeForSync(int concurrencyKey, byte[] payload) {
      throw new UnsupportedOperationException("Passive replication not supported yet.");
    }

    @Override
    public byte[] serialize(EhcacheEntityResponse response) {
      return serializeResponse(response);
    }
  };
  
  public static MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> serverMessageCodec() {
    return SERVER_INSTANCE;
  }

  public static byte[] serializeMessage(EhcacheEntityMessage message) {
    return marshall(message);
  }

  public static EhcacheEntityMessage deserializeMessage(byte[] payload) {
    return (EhcacheEntityMessage) unmarshall(payload);
  }
  
  public static byte[] serializeResponse(EhcacheEntityResponse response) {
    return marshall(response);
  }
  
  public static EhcacheEntityResponse deserializeResponse(byte[] payload) {
    return (EhcacheEntityResponse) unmarshall(payload);
  }
  
  private static Object unmarshall(byte[] payload) {
    try {
      return (EhcacheEntityMessage) new ObjectInputStream(new ByteArrayInputStream(payload)).readObject();
    } catch (IOException ex) {
      throw new IllegalArgumentException(ex);
    } catch (ClassNotFoundException ex) {
      throw new IllegalArgumentException(ex);
    }
  }
  
  private static byte[] marshall(Object message) {
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
