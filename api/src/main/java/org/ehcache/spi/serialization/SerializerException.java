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
 * Thrown by a {@link Serializer} when it cannot serialize or deserialize an instance.
 */
public class SerializerException extends RuntimeException {

  private static final long serialVersionUID = -4008956327217206643L;

  /**
   * Creates a {@code SerializerException}.
   */
  public SerializerException() {
    super();
  }

  /**
   * Creates a {@code SerializerException} with the provided message.
   *
   * @param message information about the exception
   */
  public SerializerException(final String message) {
    super(message);
  }

  /**
   * Creates a {@code SerializerException} with the provided message and cause.
   *
   * @param message information about the exception
   * @param cause the cause of this exception
   */
  public SerializerException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a {@code SerializerException} with the provided cause.
   *
   * @param cause the cause of this exception
   */
  public SerializerException(final Throwable cause) {
    super(cause);
  }

}
