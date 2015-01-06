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

package org.ehcache.exceptions;

/**
 * Exception thrown by a {@link org.ehcache.Cache} when the {@link org.ehcache.spi.loader.CacheLoader} it uses threw an
 * {@link java.lang.Exception} while loading a value for a given key
 * @author Alex Snaps
 */
public class CacheLoaderException extends RuntimeException {

  private static final long serialVersionUID = 4794738044299044587L;

  CacheLoaderException() {
    super();
  }

  CacheLoaderException(final String message) {
    super(message);
  }

  CacheLoaderException(final String message, final Throwable cause) {
    super(message, cause);
  }

  CacheLoaderException(final Throwable cause) {
    super(cause);
  }

}
