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
package org.ehcache.core.config;

import org.ehcache.expiry.ExpiryPolicy;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpiryUtilsTest {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private ExpiryPolicy<Integer, Integer> expiry;

  @Test
  public void getExpiryForCreation_valid() {
    Duration expected = Duration.ofMinutes(1);
    when(expiry.getExpiryForCreation(1, 2)).thenReturn(expected);
    Duration actual = ExpiryUtils.getExpiryForCreation(1, 2, expiry);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void getExpiryForCreation_negative() {
    Duration expected = Duration.ofMinutes(-1);
    when(expiry.getExpiryForCreation(1, 2)).thenReturn(expected);
    Duration actual = ExpiryUtils.getExpiryForCreation(1, 2, expiry);
    assertThat(actual).isEqualTo(Duration.ZERO);
  }

  @Test
  public void getExpiryForCreation_zero() {
    Duration expected = Duration.ZERO;
    when(expiry.getExpiryForCreation(1, 2)).thenReturn(expected);
    Duration actual = ExpiryUtils.getExpiryForCreation(1, 2, expiry);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void getExpiryForCreation_null() {
    when(expiry.getExpiryForCreation(1, 2)).thenReturn(null);
    Duration actual = ExpiryUtils.getExpiryForCreation(1, 2, expiry);
    assertThat(actual).isEqualTo(Duration.ZERO);
  }

  @Test
  public void getExpiryForCreation_exception() {
    when(expiry.getExpiryForCreation(1, 2)).thenThrow(new RuntimeException());
    Duration actual = ExpiryUtils.getExpiryForCreation(1, 2, expiry);
    assertThat(actual).isEqualTo(Duration.ZERO);
  }
}
