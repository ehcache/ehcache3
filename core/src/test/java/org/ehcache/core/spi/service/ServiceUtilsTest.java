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

package org.ehcache.core.spi.service;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.*;

public class ServiceUtilsTest {

  @Test
  public void findAmongstArray_notFound() {
    assertThat(ServiceUtils.findAmongst(String.class)).isEmpty();
  }

  @Test
  public void findAmongstColl_notFound() {
    assertThat(ServiceUtils.findAmongst(String.class, Collections.emptySet())).isEmpty();
  }

  @Test
  public void findAmongstArray_found() {
    assertThat(ServiceUtils.findAmongst(String.class, "test")).containsOnly("test");
  }

  @Test
  public void findAmongstColl_found() {
    assertThat(ServiceUtils.findAmongst(String.class, Collections.singleton("test"))).containsOnly("test");
  }

  @Test
  public void findSingletonAmongstArray_notFound() {
    assertThat(ServiceUtils.findSingletonAmongst(String.class)).isNull();
  }

  @Test
  public void findSingletonAmongstArray_found() {
    assertThat(ServiceUtils.findSingletonAmongst(String.class, 2, "t1")).isEqualTo("t1");
  }

  @Test
  public void findSingletonAmongstArray_twoManyFound() {
    assertThatThrownBy(() ->ServiceUtils.findSingletonAmongst(String.class, "t1", "t2"))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void findSingletonAmongstColl_notFound() {
    assertThat(ServiceUtils.findSingletonAmongst(String.class, Collections.emptySet())).isNull();
  }

  @Test
  public void findSingletonAmongstColl_found() {
    assertThat(ServiceUtils.findSingletonAmongst(String.class, Arrays.asList( 2, "t1"))).isEqualTo("t1");
  }

  @Test
  public void findSingletonAmongstColl_twoManyFound() {
    assertThatThrownBy(() ->ServiceUtils.findSingletonAmongst(String.class, Arrays.asList("t1", "t2")))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void findOptionalAmongstColl_notFound() {
    assertThat(ServiceUtils.findOptionalAmongst(String.class, Collections.emptySet())).isEmpty();
  }

  @Test
  public void findOptionalAmongstColl_found() {
    assertThat(ServiceUtils.findOptionalAmongst(String.class, Arrays.asList( 2, "t1"))).contains("t1");
  }

  @Test
  public void findOptionalAmongstArray_notFound() {
    assertThat(ServiceUtils.findOptionalAmongst(String.class)).isEmpty();
  }

  @Test
  public void findOptionalAmongstArray_found() {
    assertThat(ServiceUtils.findOptionalAmongst(String.class, 2, "t1")).contains("t1");
  }

}
