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
package org.ehcache.util;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * @author Mathieu Carbou
 */
public class DeferredTest {

  @Test(timeout = 1000)
  public void testDeferredResolvedAtCtor() throws Exception {
    Deferred<String> d = Deferred.of("hello");
    assertThat(d.get(), equalTo("hello"));
    assertThat(d.get(1, TimeUnit.MINUTES), equalTo("hello"));
    Deferred.Consumer<String> c = mock(Deferred.Consumer.class);
    d.done(c);
    verify(c).consume("hello");
  }

  @Test(timeout = 1000)
  public void testDeferredResolvedAfter() throws Exception {
    Deferred<String> d = Deferred.create();

    Deferred.Consumer<String> c = mock(Deferred.Consumer.class);
    d.done(c);

    verifyZeroInteractions(c);

    d.resolve("hello");

    verify(c).consume("hello");
    assertThat(d.get(), equalTo("hello"));
    assertThat(d.get(1, TimeUnit.MINUTES), equalTo("hello"));
  }

  @Test
  public void testDeferredTimeout() throws Exception {
    Deferred<String> d = Deferred.create();
    try {
      d.get(1, TimeUnit.SECONDS);
      fail();
    } catch (TimeoutException e) {
      assertThat(e.getMessage(), equalTo("After 1 SECONDS"));
    }
  }

  @Test
  public void testResolveOnce() throws Exception {
    Deferred<String> d = Deferred.create();
    d.resolve("");
    try {
      d.resolve("");
      fail();
    } catch (Exception e) {
      assertThat(e, instanceOf(IllegalStateException.class));
    }
  }

}
