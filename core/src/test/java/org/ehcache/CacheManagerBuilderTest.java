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

package org.ehcache;

import org.ehcache.spi.EhcachingTest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

public class CacheManagerBuilderTest {

  @Test
  public void testLoadsTheEhcache() {
    final CacheManager build = CacheManagerBuilder.newCacheManagerBuilder().build();
    assertThat(build, nullValue());
    assertThat(EhcachingTest.getInstantiationCount(), is(1));
    assertThat(EhcachingTest.getCreationCount(), is(1));
  }

}