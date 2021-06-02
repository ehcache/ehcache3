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
package org.ehcache.clustered.server.offheap;

import java.nio.ByteBuffer;
import org.terracotta.offheapstore.storage.portability.Portability;

public class LongPortability implements Portability<Long> {

  public static final Portability<Long> INSTANCE = new LongPortability();

  private LongPortability() {}

  @Override
  public ByteBuffer encode(Long value) {
    return ByteBuffer.allocate(8).putLong(0, value);
  }

  @Override
  public Long decode(ByteBuffer buffer) {
    return buffer.getLong(0);
  }

  @Override
  public boolean equals(Object value, ByteBuffer buffer) {
    if (value instanceof Long) {
      return ((Long) value) == buffer.getLong(0);
    } else {
      return false;
    }
  }

}
