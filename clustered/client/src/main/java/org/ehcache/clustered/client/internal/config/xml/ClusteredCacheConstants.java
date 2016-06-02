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

package org.ehcache.clustered.client.internal.config.xml;

import java.net.URI;
import java.net.URL;

/**
 * Constants shared between XML parsers used for clustered cache support.
 *
 * @author Clifford W. Johnson
 */
final class ClusteredCacheConstants {
  /**
   * Name of schema resource file.
   */
  static final String XSD = "ehcache-clustered-ext.xsd";
  static final URL XML_SCHEMA = ClusteredCacheConstants.class.getResource("/" + XSD);
  /**
   * Namespace for cluster configuration elements.  Must match {@code targetNamespace} in <code>{@value #XSD}</code>.
   */
  static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/clustered");
}
