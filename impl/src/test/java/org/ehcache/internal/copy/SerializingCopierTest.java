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

package org.ehcache.internal.copy;

import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by alsu on 20/08/15.
 */
public class SerializingCopierTest {

  public void testCopy() throws Exception {
    Serializer<String> serializer = mock(Serializer.class);
    String in = new String("foo");
    ByteBuffer buff = mock(ByteBuffer.class);
    when(serializer.serialize(in)).thenReturn(buff);
    when(serializer.read(buff)).thenReturn(new String("foo"));

    SerializingCopier<String> serializingCopier = new SerializingCopier<String>(serializer);
    String copied = serializingCopier.copy("foo");
    assertNotSame(in, copied);
    assertEquals(in, copied);
  }

  @Test
  public void testSerialize() throws Exception {
    Serializer<String> serializer =  mock(Serializer.class);
    SerializingCopier<String> serializingCopier = new SerializingCopier<String>(serializer);
    String in = new String("foo");
    serializingCopier.serialize(in);
    verify(serializer).serialize(in);
  }

  @Test
  public void testRead() throws Exception {
    Serializer<String> serializer =  mock(Serializer.class);
    SerializingCopier<String> serializingCopier = new SerializingCopier<String>(serializer);
    ByteBuffer buff = mock(ByteBuffer.class);
    serializingCopier.read(buff);
    verify(serializer).read(buff);
  }

  @Test
  public void testEquals() throws Exception {
    Serializer<String> serializer =  mock(Serializer.class);
    SerializingCopier<String> serializingCopier = new SerializingCopier<String>(serializer);
    String in = new String("foo");
    ByteBuffer buff = mock(ByteBuffer.class);
    serializingCopier.equals(in, buff);
    verify(serializer).equals(in, buff);
  }
}
