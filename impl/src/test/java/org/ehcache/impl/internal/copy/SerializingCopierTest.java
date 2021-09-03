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

package org.ehcache.impl.internal.copy;

import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by alsu on 20/08/15.
 */
public class SerializingCopierTest {

  @Test
  public void testCopy() throws Exception {
    @SuppressWarnings("unchecked")
    Serializer<String> serializer = mock(Serializer.class);
    String in = new String("foo");
    ByteBuffer buff = mock(ByteBuffer.class);
    when(serializer.serialize(in)).thenReturn(buff);
    when(serializer.read(buff)).thenReturn(new String("foo"));

    SerializingCopier<String> serializingCopier = new SerializingCopier<>(serializer);
    String copied = serializingCopier.copy("foo");
    assertNotSame(in, copied);
    assertEquals(in, copied);
  }

  @Test(expected = NullPointerException.class)
  public void testThrowsNPEWhenNoSerializerPassedToConstructor() {
    new SerializingCopier<>(null);
  }
}
