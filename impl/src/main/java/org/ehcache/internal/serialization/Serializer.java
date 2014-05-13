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
