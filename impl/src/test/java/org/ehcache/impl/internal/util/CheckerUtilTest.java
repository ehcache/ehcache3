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

package org.ehcache.impl.internal.util;

import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Henri Tremblay
 */
public class CheckerUtilTest {

  @Test
  public void checkKey_null() {
    assertThatThrownBy(() -> CheckerUtil.checkKey(getClass(), null))
      .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void checkKey_invalid() {
    assertThatThrownBy(() -> CheckerUtil.checkKey(getClass(), "test"))
      .isExactlyInstanceOf(ClassCastException.class)
      .hasMessage("Invalid key type, expected : " + getClass().getName() + " but was : " + String.class.getName());
  }

  @Test
  public void checkKey_valid() {
    CheckerUtil.checkKey(Object.class, "test");
  }

  @Test
  public void checkValue() {
    assertThatThrownBy(() -> CheckerUtil.checkValue(getClass(), null))
      .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void checkValue_invalid() {
    assertThatThrownBy(() -> CheckerUtil.checkValue(getClass(), "test"))
      .isExactlyInstanceOf(ClassCastException.class)
      .hasMessage("Invalid value type, expected : " + getClass().getName() + " but was : " + String.class.getName());
  }

  @Test
  public void checkValue_valid() {
    CheckerUtil.checkValue(Object.class, "test");
  }
}
