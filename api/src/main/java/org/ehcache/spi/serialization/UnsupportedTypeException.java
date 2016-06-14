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

package org.ehcache.spi.serialization;

/**
 * Exception thrown by the {@link SerializationProvider} to indicate a {@link Serializer} could not be created for a
 * given type.
 */
public class UnsupportedTypeException extends Exception {

  private static final long serialVersionUID = 4521659617291359368L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message detail message
   */
  public UnsupportedTypeException(String message) {
    super(message);
  }
}
