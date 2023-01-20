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
import org.ehcache.impl.internal.store.AbstractValueHolder;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

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
    valueHolderPortability = new OffHeapValueHolderPortability<String>(provider
        .createValueSerializer(String.class, getClass().getClassLoader()));

    originalValue = new BasicOffHeapValueHolder<String>(-1, "aValue", 1L, 2L, 3L, 0);

  }

  @Test
  public void testEncodeDecode() {
    ByteBuffer encoded = valueHolderPortability.encode(originalValue);
    OffHeapValueHolder<String> decoded = valueHolderPortability.decode(encoded);

    assertThat(originalValue, equalTo(decoded));
  }

  @Test
  public void testWriteBackSupport() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ByteBuffer encoded = valueHolderPortability.encode(originalValue);
    WriteContext writeContext = mock(WriteContext.class);
    OffHeapValueHolder<String> decoded = valueHolderPortability.decode(encoded, writeContext);

    Class<?> abstractValueHolder = AbstractValueHolder.class;
    Method setHits = abstractValueHolder.getDeclaredMethod("setHits", long.class);
    setHits.setAccessible(true);

    decoded.setExpirationTime(4L, TimeUnit.MILLISECONDS);
    decoded.setLastAccessTime(6L, TimeUnit.MILLISECONDS);
    setHits.invoke(decoded, 8L);

    decoded.writeBack();
    verify(writeContext).setLong(OffHeapValueHolderPortability.ACCESS_TIME_OFFSET, 6L);
    verify(writeContext).setLong(OffHeapValueHolderPortability.EXPIRE_TIME_OFFSET, 4L);
    verify(writeContext).setLong(OffHeapValueHolderPortability.HITS_OFFSET, 8L);
  }

}