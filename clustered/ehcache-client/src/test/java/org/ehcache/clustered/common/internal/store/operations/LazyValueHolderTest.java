/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.clustered.common.internal.store.operations;

import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class LazyValueHolderTest {

  @Mock
  private Serializer<Date> serializer;

  @Test
  public void testGetValueDecodeOnlyOnce() throws Exception {
    Date date = mock(Date.class);
    ByteBuffer buffer = ByteBuffer.allocate(0);
    doReturn(date).when(serializer).read(buffer);

    LazyValueHolder<Date> valueHolder = new LazyValueHolder<>(buffer, serializer);
    verify(serializer, never()).read(buffer); //Encoded value not deserialized on creation itself
    valueHolder.getValue();
    verify(serializer).read(buffer);  //Deserialization happens on the first invocation of getValue()
    valueHolder.getValue();
    verify(serializer).read(buffer);  //Deserialization does not repeat on subsequent getValue() calls
  }

  @Test
  public void testEncodeEncodesOnlyOnce() throws Exception {
    Date date = mock(Date.class);
    ByteBuffer buffer = ByteBuffer.allocate(0);
    doReturn(buffer).when(serializer).serialize(date);

    LazyValueHolder<Date> valueHolder = new LazyValueHolder<>(date);
    verify(serializer, never()).serialize(date); //Value not serialized on creation itself
    valueHolder.encode(serializer);
    verify(serializer).serialize(date); //Serialization happens on the first invocation of encode()
    valueHolder.encode(serializer);
    verify(serializer).serialize(date); //Serialization does not repeat on subsequent encode() calls
  }

  @Test
  public void testEncodeDoesNotEncodeAlreadyEncodedValue() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(0);

    LazyValueHolder<Date> valueHolder = new LazyValueHolder<>(buffer, serializer);
    ByteBuffer encoded = valueHolder.encode(serializer);
    assertThat(encoded.array(), sameInstance(buffer.array())); //buffer should be a dupicate to preserve positional parameters
    verify(serializer, never()).serialize(any(Date.class)); //Value not serialized as the serialized form was available on creation itself
  }
}
