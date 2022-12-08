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

import java.util.Collections;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import static org.junit.Assert.fail;
import org.junit.Test;

public class ServerSideConfigurationTest {

  @Test
  public void testNullDefaultPoolThrowsNPE() {
    try {
      new ServerSideConfiguration(null, Collections.<String, Pool>emptyMap());
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  @Test
  public void testPoolUsingDefaultWithNoDefaultThrowsIAE() {
    try {
      new ServerSideConfiguration(Collections.singletonMap("foo", new Pool(1)));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testDefaultPoolWithIllegalSize() {
    try {
      new Pool(0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testPoolWithIllegalSize() {
    try {
      new Pool(0, "foo");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }
}
