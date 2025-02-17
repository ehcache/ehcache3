/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.xml.exceptions;

import org.ehcache.javadoc.PublicApi;

/**
 * Thrown to reflect an error in an XML configuration.
 */
@PublicApi
public class XmlConfigurationException extends RuntimeException {
  private static final long serialVersionUID = 4797841652996371653L;

  public XmlConfigurationException(final String message) {
    super(message);
  }

  public XmlConfigurationException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public XmlConfigurationException(final Throwable cause) {
    super(cause);
  }
}
