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
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;

/**
 *
 * @author cdennis
 */
public class EhcacheCodec implements MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> SERVER_INSTANCE = new EhcacheCodec();

  public static MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> messageCodec() {
    return SERVER_INSTANCE;
  }

  @Override
  public byte[] encodeMessage(EhcacheEntityMessage message) {
    return marshall(message);
  }

  @Override
  public EhcacheEntityMessage decodeMessage(byte[] payload) throws MessageCodecException {
    return (EhcacheEntityMessage) unmarshall(payload);
  }

  @Override
  public byte[] encodeResponse(EhcacheEntityResponse response) throws MessageCodecException {
    return marshall(response);
  }

  @Override
  public EhcacheEntityResponse decodeResponse(byte[] payload) throws MessageCodecException {
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
