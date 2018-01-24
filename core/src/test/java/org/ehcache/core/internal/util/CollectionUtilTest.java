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

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

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

  @Test
  public void entry_value() {
    Map.Entry<Integer, String> entry = CollectionUtil.entry(1, "a");
    assertThat(entry.getKey()).isEqualTo(1);
    assertThat(entry.getValue()).isEqualTo("a");
  }

  @Test
  public void entry_nullAccepted() {
    Map.Entry<Integer, String> entry = CollectionUtil.entry(null, null);
    assertThat(entry.getKey()).isNull();
    assertThat(entry.getValue()).isNull();
  }

  @Test
  public void copyMapButFailOnNull_nullKey() {
    Map<Integer, Integer> map = CollectionUtil.map(1, 1, null, 2, 3, 3);
    assertThatExceptionOfType(NullPointerException.class)
      .isThrownBy(() -> CollectionUtil.copyMapButFailOnNull(map));
  }

  @Test
  public void copyMapButFailOnNull_nullValue() {
    Map<Integer, Integer> map = CollectionUtil.map(1, 1, 2, null, 3, 3);
    assertThatExceptionOfType(NullPointerException.class)
      .isThrownBy(() -> CollectionUtil.copyMapButFailOnNull(Collections.singletonMap(1, null)));
  }

  @Test
  public void copyMapButFailOnNull_copy() {
    Map<Integer, Long> map = CollectionUtil.map(1, 1L, 2, 2L, 3, 3L);
    assertThat(map).containsExactly(entry(1, 1L), entry(2, 2L), entry(3, 3L));
  }

  @Test
  public void map1() {
    Map<Integer, Long> map = CollectionUtil.map(1, 1L);
    assertThat(map).containsExactly(entry(1, 1L));
  }

  @Test
  public void map2() {
    Map<Integer, Long> map = CollectionUtil.map(1, 1L, 2, 2L);
    assertThat(map).containsExactly(entry(1, 1L), entry(2, 2L));
  }

  @Test
  public void map3() {
    Map<Integer, Long> map = CollectionUtil.map(1, 1L, 2, 2L, 3, 3L);
    assertThat(map).containsExactly(entry(1, 1L), entry(2, 2L), entry(3, 3L));
  }
}
