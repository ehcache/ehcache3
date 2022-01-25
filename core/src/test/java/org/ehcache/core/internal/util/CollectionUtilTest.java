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
package org.ehcache.core.internal.util;

import org.ehcache.core.util.CollectionUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.*;

public class CollectionUtilTest {

  @Test
  public void findBestCollectionSize_sizeable() {
    int size = CollectionUtil.findBestCollectionSize(Arrays.asList(1, 2 ,3), 100);
    assertThat(size).isEqualTo(3);
  }

  @Test
  public void findBestCollectionSize_empty() {
    int size = CollectionUtil.findBestCollectionSize(Collections.emptySet(), 100);
    assertThat(size).isZero();
  }

  @Test
  public void findBestCollectionSize_singleton() {
    int size = CollectionUtil.findBestCollectionSize(Collections.singleton(1), 100);
    assertThat(size).isEqualTo(1);
  }

  @Test
  public void findBestCollectionSize_notSizeable() {
    int size = CollectionUtil.findBestCollectionSize(() -> null, 100);
    assertThat(size).isEqualTo(100);
  }
}
