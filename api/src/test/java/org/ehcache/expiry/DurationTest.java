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
package org.ehcache.expiry;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class DurationTest {

  @Test
  public void testBasic() {
    Duration duration = new Duration(1, TimeUnit.SECONDS);
    assertThat(duration.getLength(), equalTo(1L));
    assertThat(duration.getTimeUnit(), equalTo(TimeUnit.SECONDS));
    assertThat(duration.isInfinite(), equalTo(false));
  }

  @Test
  public void testExplicitZero() {
    Duration zero1 = new Duration(0, TimeUnit.SECONDS);
    Duration zero2 = new Duration(0, TimeUnit.MILLISECONDS);
    assertThat(zero1.equals(zero2), equalTo(true));
    assertThat(zero2.equals(zero1), equalTo(true));
    assertThat(zero1.hashCode() == zero2.hashCode(), equalTo(true));
  }

  @Test
  public void testEqualsHashcode() {
    Set<Duration> set = new HashSet<Duration>();
    assertAdd(Duration.INFINITE, set);
    assertAdd(Duration.ZERO, set);
    assertThat(set.add(new Duration(0L, TimeUnit.SECONDS)), equalTo(false));
    assertAdd(new Duration(1L, TimeUnit.SECONDS), set);
    assertAdd(new Duration(1L, TimeUnit.MILLISECONDS), set);
    assertAdd(new Duration(42L, TimeUnit.SECONDS), set);
    assertAdd(new Duration(43L, TimeUnit.SECONDS), set);
  }

  private void assertAdd(Duration duration, Set<Duration> set) {
    assertThat(set.add(duration), equalTo(true));
    assertThat(set.add(duration), equalTo(false));
  }

  @Test
  public void testForever() {
    assertThat(Duration.INFINITE.isInfinite(), equalTo(true));
    assertThat(Duration.INFINITE.equals(Duration.ZERO), is(false));

    try {
      Duration.INFINITE.getLength();
      throw new AssertionError();
    } catch (IllegalStateException ise) {
      // expected
    }

    try {
      Duration.INFINITE.getTimeUnit();
      throw new AssertionError();
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testZero() {
    assertThat(Duration.ZERO.isInfinite(), equalTo(false));
    assertThat(Duration.ZERO.getLength(), equalTo(0L));
    assertThat(Duration.ZERO.getTimeUnit(), any(TimeUnit.class));
    assertThat(Duration.ZERO.equals(Duration.INFINITE), is(false));
  }

}
