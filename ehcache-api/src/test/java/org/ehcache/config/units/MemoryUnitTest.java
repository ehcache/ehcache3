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
package org.ehcache.config.units;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.OrderingComparison.lessThan;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class MemoryUnitTest {

  @Test
  public void testBasicPositiveConversions() {
    assertThat(MemoryUnit.B.toBytes(1L), is(1L));
    assertThat(MemoryUnit.KB.toBytes(1L), is(1024L));
    assertThat(MemoryUnit.MB.toBytes(1L), is(1024L * 1024L));
    assertThat(MemoryUnit.GB.toBytes(1L), is(1024L * 1024L * 1024L));
    assertThat(MemoryUnit.TB.toBytes(1L), is(1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.PB.toBytes(1L), is(1024L * 1024L * 1024L * 1024L * 1024L));

    assertThat(MemoryUnit.B.convert(1L, MemoryUnit.B), is(1L));
    assertThat(MemoryUnit.KB.convert(1L, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.MB.convert(1L, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.GB.convert(1L, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.TB.convert(1L, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.PB.convert(1L, MemoryUnit.B), is(0L));

    assertThat(MemoryUnit.B.convert(1L, MemoryUnit.KB), is(1024L));
    assertThat(MemoryUnit.KB.convert(1L, MemoryUnit.KB), is(1L));
    assertThat(MemoryUnit.MB.convert(1L, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.GB.convert(1L, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.TB.convert(1L, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.PB.convert(1L, MemoryUnit.KB), is(0L));

    assertThat(MemoryUnit.B.convert(1L, MemoryUnit.MB), is(1024L * 1024L));
    assertThat(MemoryUnit.KB.convert(1L, MemoryUnit.MB), is(1024L));
    assertThat(MemoryUnit.MB.convert(1L, MemoryUnit.MB), is(1L));
    assertThat(MemoryUnit.GB.convert(1L, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.TB.convert(1L, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.PB.convert(1L, MemoryUnit.MB), is(0L));

    assertThat(MemoryUnit.B.convert(1L, MemoryUnit.GB), is(1024L * 1024L * 1024L));
    assertThat(MemoryUnit.KB.convert(1L, MemoryUnit.GB), is(1024L * 1024L));
    assertThat(MemoryUnit.MB.convert(1L, MemoryUnit.GB), is(1024L));
    assertThat(MemoryUnit.GB.convert(1L, MemoryUnit.GB), is(1L));
    assertThat(MemoryUnit.TB.convert(1L, MemoryUnit.GB), is(0L));
    assertThat(MemoryUnit.PB.convert(1L, MemoryUnit.GB), is(0L));

    assertThat(MemoryUnit.B.convert(1L, MemoryUnit.TB), is(1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.KB.convert(1L, MemoryUnit.TB), is(1024L * 1024L * 1024L));
    assertThat(MemoryUnit.MB.convert(1L, MemoryUnit.TB), is(1024L * 1024L));
    assertThat(MemoryUnit.GB.convert(1L, MemoryUnit.TB), is(1024L));
    assertThat(MemoryUnit.TB.convert(1L, MemoryUnit.TB), is(1L));
    assertThat(MemoryUnit.PB.convert(1L, MemoryUnit.TB), is(0L));

    assertThat(MemoryUnit.B.convert(1L, MemoryUnit.PB), is(1024L * 1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.KB.convert(1L, MemoryUnit.PB), is(1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.MB.convert(1L, MemoryUnit.PB), is(1024L * 1024L * 1024L));
    assertThat(MemoryUnit.GB.convert(1L, MemoryUnit.PB), is(1024L * 1024L));
    assertThat(MemoryUnit.TB.convert(1L, MemoryUnit.PB), is(1024L));
    assertThat(MemoryUnit.PB.convert(1L, MemoryUnit.PB), is(1L));
  }

  @Test
  public void testBasicNegativeConversions() {
    assertThat(MemoryUnit.B.toBytes(-1L), is(-1L));
    assertThat(MemoryUnit.KB.toBytes(-1L), is(-1024L));
    assertThat(MemoryUnit.MB.toBytes(-1L), is(-1024L * 1024L));
    assertThat(MemoryUnit.GB.toBytes(-1L), is(-1024L * 1024L * 1024L));
    assertThat(MemoryUnit.TB.toBytes(-1L), is(-1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.PB.toBytes(-1L), is(-1024L * 1024L * 1024L * 1024L * 1024L));

    assertThat(MemoryUnit.B.convert(-1L, MemoryUnit.B), is(-1L));
    assertThat(MemoryUnit.KB.convert(-1L, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.MB.convert(-1L, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.GB.convert(-1L, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.TB.convert(-1L, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.PB.convert(-1L, MemoryUnit.B), is(0L));

    assertThat(MemoryUnit.B.convert(-1L, MemoryUnit.KB), is(-1024L));
    assertThat(MemoryUnit.KB.convert(-1L, MemoryUnit.KB), is(-1L));
    assertThat(MemoryUnit.MB.convert(-1L, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.GB.convert(-1L, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.TB.convert(-1L, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.PB.convert(-1L, MemoryUnit.KB), is(0L));

    assertThat(MemoryUnit.B.convert(-1L, MemoryUnit.MB), is(-1024L * 1024L));
    assertThat(MemoryUnit.KB.convert(-1L, MemoryUnit.MB), is(-1024L));
    assertThat(MemoryUnit.MB.convert(-1L, MemoryUnit.MB), is(-1L));
    assertThat(MemoryUnit.GB.convert(-1L, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.TB.convert(-1L, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.PB.convert(-1L, MemoryUnit.MB), is(0L));

    assertThat(MemoryUnit.B.convert(-1L, MemoryUnit.GB), is(-1024L * 1024L * 1024L));
    assertThat(MemoryUnit.KB.convert(-1L, MemoryUnit.GB), is(-1024L * 1024L));
    assertThat(MemoryUnit.MB.convert(-1L, MemoryUnit.GB), is(-1024L));
    assertThat(MemoryUnit.GB.convert(-1L, MemoryUnit.GB), is(-1L));
    assertThat(MemoryUnit.TB.convert(-1L, MemoryUnit.GB), is(0L));
    assertThat(MemoryUnit.PB.convert(-1L, MemoryUnit.GB), is(0L));

    assertThat(MemoryUnit.B.convert(-1L, MemoryUnit.TB), is(-1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.KB.convert(-1L, MemoryUnit.TB), is(-1024L * 1024L * 1024L));
    assertThat(MemoryUnit.MB.convert(-1L, MemoryUnit.TB), is(-1024L * 1024L));
    assertThat(MemoryUnit.GB.convert(-1L, MemoryUnit.TB), is(-1024L));
    assertThat(MemoryUnit.TB.convert(-1L, MemoryUnit.TB), is(-1L));
    assertThat(MemoryUnit.PB.convert(-1L, MemoryUnit.TB), is(0L));

    assertThat(MemoryUnit.B.convert(-1L, MemoryUnit.PB), is(-1024L * 1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.KB.convert(-1L, MemoryUnit.PB), is(-1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.MB.convert(-1L, MemoryUnit.PB), is(-1024L * 1024L * 1024L));
    assertThat(MemoryUnit.GB.convert(-1L, MemoryUnit.PB), is(-1024L * 1024L));
    assertThat(MemoryUnit.TB.convert(-1L, MemoryUnit.PB), is(-1024L));
    assertThat(MemoryUnit.PB.convert(-1L, MemoryUnit.PB), is(-1L));
  }

  @Test
  public void testPositiveThresholdConditions() {
    assertThat(MemoryUnit.KB.convert(1024L - 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.KB.convert(1024L + 1, MemoryUnit.B), is(1L));
    assertThat(MemoryUnit.MB.convert((1024L * 1024L) - 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.MB.convert((1024L * 1024L) + 1, MemoryUnit.B), is(1L));
    assertThat(MemoryUnit.GB.convert((1024L * 1024L * 1024L) - 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.GB.convert((1024L * 1024L * 1024L) + 1, MemoryUnit.B), is(1L));
    assertThat(MemoryUnit.TB.convert((1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.TB.convert((1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.B), is(1L));
    assertThat(MemoryUnit.PB.convert((1024L * 1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.PB.convert((1024L * 1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.B), is(1L));

    assertThat(MemoryUnit.MB.convert(1024L - 1, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.MB.convert(1024L + 1, MemoryUnit.KB), is(1L));
    assertThat(MemoryUnit.GB.convert((1024L * 1024L) - 1, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.GB.convert((1024L * 1024L) + 1, MemoryUnit.KB), is(1L));
    assertThat(MemoryUnit.TB.convert((1024L * 1024L * 1024L) - 1, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.TB.convert((1024L * 1024L * 1024L) + 1, MemoryUnit.KB), is(1L));
    assertThat(MemoryUnit.PB.convert((1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.PB.convert((1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.KB), is(1L));

    assertThat(MemoryUnit.GB.convert(1024L - 1, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.GB.convert(1024L + 1, MemoryUnit.MB), is(1L));
    assertThat(MemoryUnit.TB.convert((1024L * 1024L) - 1, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.TB.convert((1024L * 1024L) + 1, MemoryUnit.MB), is(1L));
    assertThat(MemoryUnit.PB.convert((1024L * 1024L * 1024L) - 1, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.PB.convert((1024L * 1024L * 1024L) + 1, MemoryUnit.MB), is(1L));

    assertThat(MemoryUnit.TB.convert(1024L - 1, MemoryUnit.GB), is(0L));
    assertThat(MemoryUnit.TB.convert(1024L + 1, MemoryUnit.GB), is(1L));
    assertThat(MemoryUnit.PB.convert((1024L * 1024L) - 1, MemoryUnit.GB), is(0L));
    assertThat(MemoryUnit.PB.convert((1024L * 1024L) + 1, MemoryUnit.GB), is(1L));

    assertThat(MemoryUnit.PB.convert(1024L - 1, MemoryUnit.TB), is(0L));
    assertThat(MemoryUnit.PB.convert(1024L + 1, MemoryUnit.TB), is(1L));
  }

  @Test
  public void testNegativeThresholdConditions() {
    assertThat(MemoryUnit.KB.convert(-1024L + 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.KB.convert(-1024L - 1, MemoryUnit.B), is(-1L));
    assertThat(MemoryUnit.MB.convert(-(1024L * 1024L) + 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.MB.convert(-(1024L * 1024L) - 1, MemoryUnit.B), is(-1L));
    assertThat(MemoryUnit.GB.convert(-(1024L * 1024L * 1024L) + 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.GB.convert(-(1024L * 1024L * 1024L) - 1, MemoryUnit.B), is(-1L));
    assertThat(MemoryUnit.TB.convert(-(1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.TB.convert(-(1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.B), is(-1L));
    assertThat(MemoryUnit.PB.convert(-(1024L * 1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.B), is(0L));
    assertThat(MemoryUnit.PB.convert(-(1024L * 1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.B), is(-1L));

    assertThat(MemoryUnit.MB.convert(-1024L + 1, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.MB.convert(-1024L - 1, MemoryUnit.KB), is(-1L));
    assertThat(MemoryUnit.GB.convert(-(1024L * 1024L) + 1, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.GB.convert(-(1024L * 1024L) - 1, MemoryUnit.KB), is(-1L));
    assertThat(MemoryUnit.TB.convert(-(1024L * 1024L * 1024L) + 1, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.TB.convert(-(1024L * 1024L * 1024L) - 1, MemoryUnit.KB), is(-1L));
    assertThat(MemoryUnit.PB.convert(-(1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.KB), is(0L));
    assertThat(MemoryUnit.PB.convert(-(1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.KB), is(-1L));

    assertThat(MemoryUnit.GB.convert(-1024L + 1, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.GB.convert(-1024L - 1, MemoryUnit.MB), is(-1L));
    assertThat(MemoryUnit.TB.convert(-(1024L * 1024L) + 1, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.TB.convert(-(1024L * 1024L) - 1, MemoryUnit.MB), is(-1L));
    assertThat(MemoryUnit.PB.convert(-(1024L * 1024L * 1024L) + 1, MemoryUnit.MB), is(0L));
    assertThat(MemoryUnit.PB.convert(-(1024L * 1024L * 1024L) - 1, MemoryUnit.MB), is(-1L));

    assertThat(MemoryUnit.TB.convert(-1024L + 1, MemoryUnit.GB), is(0L));
    assertThat(MemoryUnit.TB.convert(-1024L - 1, MemoryUnit.GB), is(-1L));
    assertThat(MemoryUnit.PB.convert(-(1024L * 1024L) + 1, MemoryUnit.GB), is(0L));
    assertThat(MemoryUnit.PB.convert(-(1024L * 1024L) - 1, MemoryUnit.GB), is(-1L));

    assertThat(MemoryUnit.PB.convert(-1024L + 1, MemoryUnit.TB), is(0L));
    assertThat(MemoryUnit.PB.convert(-1024L - 1, MemoryUnit.TB), is(-1L));
  }

  @Test
  public void testThresholdComparisons() {
    assertThat(MemoryUnit.KB.compareTo(1, 1024L - 1, MemoryUnit.B), greaterThan(0));
    assertThat(MemoryUnit.KB.compareTo(1, 1024L, MemoryUnit.B), is(0));
    assertThat(MemoryUnit.KB.compareTo(1, 1024L + 1, MemoryUnit.B), lessThan(0));
    assertThat(MemoryUnit.KB.compareTo(2, 2048L - 1, MemoryUnit.B), greaterThan(0));
    assertThat(MemoryUnit.KB.compareTo(2, 2048L, MemoryUnit.B), is(0));
    assertThat(MemoryUnit.KB.compareTo(2, 2048L + 1, MemoryUnit.B), lessThan(0));
  }
}
