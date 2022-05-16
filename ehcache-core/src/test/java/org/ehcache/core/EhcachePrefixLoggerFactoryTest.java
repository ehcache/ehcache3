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

package org.ehcache.core;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class EhcachePrefixLoggerFactoryTest {

  @Test
  public void testContextLeakUseCase() throws InterruptedException {

    EhcachePrefixLoggerFactory.Context context = EhcachePrefixLoggerFactory.withContext("cache-name", "my-cache");

    Thread t1 = new Thread(() -> {

      context.close();
      assertThat("Unable to close the context", EhcachePrefixLoggerFactory.getContextMap(), nullValue());
    });

    t1.start();
    t1.join();

    assertThat("Context is been already closed", EhcachePrefixLoggerFactory.getContextMap(), notNullValue());
  }

}
