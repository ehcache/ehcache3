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

public class TestClientSourceId implements ClientSourceId {

  private final long id;

  public TestClientSourceId(long id) {
    this.id = id;
  }

  @Override
  public long toLong() {
    return id;
  }

  @Override
  public boolean matches(ClientDescriptor clientDescriptor) {
    return clientDescriptor.getSourceId().toLong() == id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TestClientSourceId that = (TestClientSourceId)o;

    return id == that.id;
  }

  @Override
  public int hashCode() {
    return (int)(id ^ (id >>> 32));
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + id + ")";
  }
}
