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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngineConfigurationTest {
  
  @Test
  public void testIllegalMaxDepthArgument() {
    try {
      new DefaultSizeOfEngineConfiguration(0, 1l);
      fail();
    } catch (Exception illegalArgument) {
      assertThat(illegalArgument, instanceOf(IllegalArgumentException.class));
      assertThat(illegalArgument.getMessage(), equalTo("MaxDepth/MaxSize can only accept positive values."));
    }
  }
  
  @Test
  public void testIllegalMaxSizeArgument() {
    try {
      new DefaultSizeOfEngineConfiguration(1l, 0);
      fail();
    } catch (Exception illegalArgument) {
      assertThat(illegalArgument, instanceOf(IllegalArgumentException.class));
      assertThat(illegalArgument.getMessage(), equalTo("MaxDepth/MaxSize can only accept positive values."));
    }
  }
  
  @Test
  public void testValidArguments() {
    DefaultSizeOfEngineConfiguration configuration = new DefaultSizeOfEngineConfiguration(10l, 10l);
    assertThat(configuration.getMaxDepth(), equalTo(10l));
    assertThat(configuration.getMaxSize(), equalTo(10l));
  }

}
