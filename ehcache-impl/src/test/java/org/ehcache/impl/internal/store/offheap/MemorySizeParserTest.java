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

package org.ehcache.impl.internal.store.offheap;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class MemorySizeParserTest {

  @Test
  public void testParse() {
    assertEquals(0, MemorySizeParser.parse("0"));
    assertEquals(0, MemorySizeParser.parse(""));
    assertEquals(0, MemorySizeParser.parse(null));
    assertEquals(10, MemorySizeParser.parse("10"));
    assertEquals(4096, MemorySizeParser.parse("4k"));
    assertEquals(4096, MemorySizeParser.parse("4K"));
    assertEquals(16777216, MemorySizeParser.parse("16m"));
    assertEquals(16777216, MemorySizeParser.parse("16M"));
    assertEquals(2147483648L, MemorySizeParser.parse("2g"));
    assertEquals(2147483648L, MemorySizeParser.parse("2G"));
    assertEquals(3298534883328L, MemorySizeParser.parse("3t"));
    assertEquals(3298534883328L, MemorySizeParser.parse("3T"));
  }

  @Test
  public void testParseErrors() {
    try {
      MemorySizeParser.parse("-1G");
      Assert.fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      MemorySizeParser.parse("1000y");
      Assert.fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
