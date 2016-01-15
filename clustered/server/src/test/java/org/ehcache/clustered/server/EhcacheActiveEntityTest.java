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
package org.ehcache.clustered.server;

import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.fail;

/**
 *
 * @author cdennis
 */
public class EhcacheActiveEntityTest {
  
  @Test
  public void testUUIDConfigSerialization() {
    byte[] config = new byte[16];
    new Random().nextBytes(config);
    
    EhcacheActiveEntity entity = new EhcacheActiveEntity(config);
    
    assertThat(entity.getConfig(), equalTo(config));
  }

  @Test
  public void testConfigTooShort() {
    try {
      new EhcacheActiveEntity(new byte[15]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testConfigTooLong() {
    try {
      new EhcacheActiveEntity(new byte[17]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }
  
  @Test
  public void testConfigNull() {
    try {
      new EhcacheActiveEntity(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }
}
