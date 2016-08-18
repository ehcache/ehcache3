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

package org.ehcache.clustered.common;

import java.util.Random;
import java.util.UUID;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.junit.Test;

public class ClusteredEhcacheIdentityTest {

  @Test
  public void testUUIDSerialization() {
    byte[] config = new byte[16];
    new Random().nextBytes(config);

    UUID uuid = ClusteredEhcacheIdentity.deserialize(config);

    assertThat(ClusteredEhcacheIdentity.serialize(uuid), equalTo(config));
  }

  @Test
  public void testDeserializeTooShort() {
    try {
      ClusteredEhcacheIdentity.deserialize(new byte[15]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testDeserializeTooLong() {
    try {
      ClusteredEhcacheIdentity.deserialize(new byte[17]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testDeserializeNull() {
    try {
      ClusteredEhcacheIdentity.deserialize(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  @Test
  public void testSerializeNull() {
    try {
      ClusteredEhcacheIdentity.serialize(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }
}
