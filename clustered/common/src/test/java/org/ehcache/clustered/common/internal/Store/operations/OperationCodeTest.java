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
package org.ehcache.clustered.common.internal.Store.operations;

import org.ehcache.clustered.common.internal.store.operations.OperationCode;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class OperationCodeTest {

  @Test
  public void testPinning() {
    assertThat(OperationCode.PUT.shouldBePinned(), is(false));

    Arrays.stream(OperationCode.values())
          .filter(operationCode -> operationCode != OperationCode.PUT)
          .forEach((operationCode -> assertThat(operationCode + " must be pinned", operationCode.shouldBePinned(), is(true))));
  }
}
