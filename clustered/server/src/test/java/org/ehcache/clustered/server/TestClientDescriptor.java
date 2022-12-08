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

package org.ehcache.clustered.server;

import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ClientSourceId;

import java.util.concurrent.atomic.AtomicInteger;

public final class TestClientDescriptor implements ClientDescriptor {
  private static final AtomicInteger counter = new AtomicInteger(0);

  private final int clientId = counter.incrementAndGet();

  public static ClientDescriptor create() {
    return new TestClientDescriptor();
  }

  @Override
  public ClientSourceId getSourceId() {
    return new TestClientSourceId(clientId);
  }

  @Override
  public String toString() {
    return "TestClientDescriptor[" + clientId + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestClientDescriptor that = (TestClientDescriptor) o;

    return clientId == that.clientId;
  }

  @Override
  public int hashCode() {
    return clientId;
  }
}
