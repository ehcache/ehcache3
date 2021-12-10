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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.impl.serialization.JavaSerializer;
import org.junit.Test;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * LazyOffHeapValueHolderTest
 */
public class LazyOffHeapValueHolderTest {

  @Test
  public void testDelayedDeserialization() {
    JavaSerializer<String> serializer = new JavaSerializer<>(getClass().getClassLoader());
    String testValue = "Let's get binary!";
    ByteBuffer serialized = serializer.serialize(testValue);
    OffHeapValueHolder<String> valueHolder = new LazyOffHeapValueHolder<>(1L, serialized, serializer, 10L, 20L, 15L, 3, mock(WriteContext.class));

    valueHolder.detach();
    serialized.clear();
    assertThat(valueHolder.get(), is(testValue));
  }

  @Test
  public void testCanAccessBinaryValue() throws ClassNotFoundException {
    JavaSerializer<String> serializer = new JavaSerializer<>(getClass().getClassLoader());
    String testValue = "Let's get binary!";
    ByteBuffer serialized = serializer.serialize(testValue);
    LazyOffHeapValueHolder<String> valueHolder = new LazyOffHeapValueHolder<>(1L, serialized, serializer, 10L, 20L, 15L, 3, mock(WriteContext.class));

    valueHolder.detach();

    ByteBuffer binaryValue = valueHolder.getBinaryValue();
    assertThat(serializer.read(binaryValue), is(testValue));
  }

  @Test
  public void testPreventAccessToBinaryValueIfNotPrepared() {
    JavaSerializer<String> serializer = new JavaSerializer<>(getClass().getClassLoader());
    String testValue = "Let's get binary!";
    ByteBuffer serialized = serializer.serialize(testValue);
    LazyOffHeapValueHolder<String> valueHolder = new LazyOffHeapValueHolder<>(1L, serialized, serializer, 10L, 20L, 15L, 3, mock(WriteContext.class));

    try {
      valueHolder.getBinaryValue();
      fail("IllegalStateException expected");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("has not been prepared"));
    }
  }
}
