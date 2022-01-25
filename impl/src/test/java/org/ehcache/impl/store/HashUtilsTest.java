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

package org.ehcache.impl.store;

import org.junit.Test;

import java.util.Random;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * HashUtilsTest
 */
public class HashUtilsTest {

  @Test
  public void testHashTransform() {
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      int hash = random.nextInt();
      long longHash = HashUtils.intHashToLong(hash);
      int inthash = HashUtils.longHashToInt(longHash);

      assertThat(inthash, is(hash));
    }
  }

}
