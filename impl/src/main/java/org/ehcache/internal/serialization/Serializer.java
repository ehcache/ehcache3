/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.internal.serialization;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public interface Serializer<T> {
  
  ByteBuffer serialize(T object) throws IOException;
  
  T read(ByteBuffer binary) throws IOException, ClassNotFoundException;
  
  boolean equals(T object, ByteBuffer binary) throws IOException, ClassNotFoundException;

}
