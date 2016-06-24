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

package org.ehcache.clustered.client.internal;

/**
 * Thrown to indicate a failure in creating an {@code Entity} supporting clustered operations.
 */
public class EhcacheEntityCreationException extends RuntimeException {
  private static final long serialVersionUID = -7615471321129233710L;

  public EhcacheEntityCreationException(String message) {
    super(message);
  }

  public EhcacheEntityCreationException(String message, Throwable cause) {
    super(message, cause);
  }

  public EhcacheEntityCreationException(Throwable cause) {
    super(cause);
  }
}
