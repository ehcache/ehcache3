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

package org.ehcache.internal.sizeof;

import org.ehcache.internal.sizeof.listeners.exceptions.VisitorListenerException;
import org.ehcache.spi.sizeof.SizeOfEngine;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngineTest {

  @Test
  public void testMaxDepthReachedVisitorListenerException() {
    SizeOfEngine sizeOfEngine = new DefaultSizeOfEngine(3, Long.MAX_VALUE);
    try {
      sizeOfEngine.sizeof(new MaxDepthGreaterThanThree());
      fail();
    } catch (Exception visitorException) {
      assertThat(visitorException, instanceOf(VisitorListenerException.class));
      assertThat(visitorException.getMessage(), containsString("Max Depth reached for the object"));
    }
  }
  
  @Test
  public void testMaxSizeReachedVisitorListenerException() {
    SizeOfEngine sizeOfEngine = new DefaultSizeOfEngine(Long.MAX_VALUE, 1000);
    try {
      String overSized = new String(new byte[1000]);
      sizeOfEngine.sizeof(overSized);
      fail();
    } catch (Exception visitorException) {
      assertThat(visitorException, instanceOf(VisitorListenerException.class));
      assertThat(visitorException.getMessage(), containsString("Max Size reached for the object"));
    }
  }
  private static class MaxDepthGreaterThanThree {
    private Object second = new Object();
    private Object third = new Object();
    private Object fourth = new Object();
  }
}
