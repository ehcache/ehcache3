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

package org.ehcache.transactions.xa.internal;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * XAValueHolderTest
 */
public class XAValueHolderTest {

  @Test
  public void testSerialization() throws Exception {

    long now = System.currentTimeMillis();
    XAValueHolder<String> valueHolder = new XAValueHolder<>("value", now - 1000);
    valueHolder.accessed(now, Duration.ofSeconds(100));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream outputStream = new ObjectOutputStream(baos);
    outputStream.writeObject(valueHolder);
    outputStream.close();

    @SuppressWarnings("unchecked")
    XAValueHolder<String> result = (XAValueHolder<String>) new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())).readObject();

    assertThat(result.getId(), is(valueHolder.getId()));
    assertThat(result.creationTime(), is(valueHolder.creationTime()));
    assertThat(result.lastAccessTime(), is(valueHolder.lastAccessTime()));
    assertThat(result.expirationTime(), is(valueHolder.expirationTime()));
    assertThat(result.get(), is(valueHolder.get()));
  }
}
