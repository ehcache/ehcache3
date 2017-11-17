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

import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Henri Tremblay
 */
public class OffHeapStoreUtilsTest {

  private static final String PROPERTY = "stuff";

  @After
  public void tearDown() {
    System.getProperties().remove(OffHeapStoreUtils.PATH_PREFIX + PROPERTY);
  }

  private void setProperty(String value) {
    System.setProperty(OffHeapStoreUtils.PATH_PREFIX + PROPERTY, value);
  }

  @Test
  public void getAdvancedMemorySizeConfigProperty_foundMb() throws Exception {
    setProperty("20M");
    long value = OffHeapStoreUtils.getAdvancedMemorySizeConfigProperty(PROPERTY, 10L);
    assertThat(value).isEqualTo(20L * 1024 * 1024);
  }

  @Test
  public void getAdvancedMemorySizeConfigProperty_foundNoUnit() throws Exception {
    setProperty("20");
    long value = OffHeapStoreUtils.getAdvancedMemorySizeConfigProperty(PROPERTY, 10L);
    assertThat(value).isEqualTo(20L);
  }

  @Test
  public void getAdvancedMemorySizeConfigProperty_notFound() throws Exception {
    long value = OffHeapStoreUtils.getAdvancedMemorySizeConfigProperty(PROPERTY, 10L);
    assertThat(value).isEqualTo(10L);
  }

  @Test
  public void getAdvancedLongConfigProperty_found() throws Exception {
    setProperty("20");
    long value = OffHeapStoreUtils.getAdvancedLongConfigProperty(PROPERTY, 10L);
    assertThat(value).isEqualTo(20L);
  }

  @Test
  public void getAdvancedLongConfigProperty_notFound() throws Exception {
    long value = OffHeapStoreUtils.getAdvancedLongConfigProperty(PROPERTY, 10L);
    assertThat(value).isEqualTo(10L);
  }

  @Test
  public void getAdvancedBooleanConfigProperty_found() throws Exception {
    setProperty("true");
    boolean value = OffHeapStoreUtils.getAdvancedBooleanConfigProperty(PROPERTY, false);
    assertThat(value).isTrue();
  }

  @Test
  public void getAdvancedBooleanConfigProperty_notFound() throws Exception {
    boolean value = OffHeapStoreUtils.getAdvancedBooleanConfigProperty(PROPERTY, true);
    assertThat(value).isTrue();
  }
}
