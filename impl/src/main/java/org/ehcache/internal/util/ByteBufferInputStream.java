/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class ByteBufferInputStream extends InputStream {

  private final ByteBuffer buffer;
  
  public ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer.slice();
  }
  
  @Override
  public int read() throws IOException {
    if (buffer.hasRemaining()) {
      return 0xff & buffer.get();
    } else {
      return -1;
    }
  }
}
