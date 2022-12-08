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
package org.ehcache.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class ByteBufferInputStream extends InputStream {

  private final ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer.slice();
  }

  @Override
  public int read() {
    if (buffer.hasRemaining()) {
      return 0xff & buffer.get();
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte b[], int off, int len) {
    len = min(len, buffer.remaining());
    buffer.get(b, off, len);
    return len;
  }

  @Override
  public long skip(long n) {
    n = min(buffer.remaining(), max(n, 0));
    buffer.position((int) (buffer.position() + n));
    return n;
  }

  @Override
  public synchronized int available() {
    return buffer.remaining();
  }
}
