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

import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class OffHeapValueHolderPortabilityTest {

  private OffHeapValueHolderPortability<String> valueHolderPortability;
  private OffHeapValueHolder<String> originalValue;

  @Before
  public void setup() throws UnsupportedTypeException {
    SerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());
    valueHolderPortability = new OffHeapValueHolderPortability<>(provider
      .createValueSerializer(String.class, getClass().getClassLoader()));

    originalValue = new BasicOffHeapValueHolder<>(-1, "aValue", 1L, 2L, 3L);

  }

  @Test
  public void testEncodeDecode() throws IOException {
    ByteBuffer encoded = valueHolderPortability.encode(originalValue);

    // uncomment to perform backward compatibility tests
    // passThroughAFile(encoded);

    OffHeapValueHolder<String> decoded = valueHolderPortability.decode(encoded);

    assertThat(originalValue, equalTo(decoded));
  }

  /**
   * This method can be used to test backward compatibility punctually. You run the test once and it will save the content
   * of the buffer to a file. You then run it again with the new version by commenting the file writing. If it works,
   * it means you are backward compatible.
   *
   * @param encoded the buffer to save to file
   * @throws IOException if something goes wrong
   */
  private void passThroughAFile(ByteBuffer encoded) throws IOException {
    Path path = Paths.get("build/offheap.dat");
    Files.write(path, encoded.array()); // comment this line when running the second time
    encoded.position(0);
    encoded.put(Files.readAllBytes(path));
    encoded.flip();
  }

  @Test
  public void testDecodingAPreviousVersionWithTheHits() {
    StringSerializer serializer = new StringSerializer();
    ByteBuffer serialized = serializer.serialize("test");

    long time = System.currentTimeMillis();

    ByteBuffer byteBuffer = ByteBuffer.allocate(serialized.remaining() + 40);
    byteBuffer.putLong(123L); // id
    byteBuffer.putLong(time); // creation time
    byteBuffer.putLong(time + 1); // last access time
    byteBuffer.putLong(time + 2); // expiration time
    byteBuffer.putLong(100L); // hits
    byteBuffer.put(serialized); // the held value
    byteBuffer.flip();

    OffHeapValueHolder<String> decoded = valueHolderPortability.decode(byteBuffer);
    assertThat(decoded.getId(), equalTo(123L));
    assertThat(decoded.creationTime(), equalTo(time));
    assertThat(decoded.lastAccessTime(), equalTo(time + 1));
    assertThat(decoded.expirationTime(), equalTo(time + 2));
    assertThat(decoded.get(), equalTo("test"));
  }

  @Test
  public void testWriteBackSupport() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ByteBuffer encoded = valueHolderPortability.encode(originalValue);
    WriteContext writeContext = mock(WriteContext.class);
    OffHeapValueHolder<String> decoded = valueHolderPortability.decode(encoded, writeContext);

    decoded.setExpirationTime(4L);
    decoded.setLastAccessTime(6L);
    decoded.writeBack();

    verify(writeContext).setLong(OffHeapValueHolderPortability.ACCESS_TIME_OFFSET, 6L);
    verify(writeContext).setLong(OffHeapValueHolderPortability.EXPIRE_TIME_OFFSET, 4L);
  }

}
