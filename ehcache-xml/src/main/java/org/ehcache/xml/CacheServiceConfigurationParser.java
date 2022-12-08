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

package org.ehcache.xml;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;

import javax.xml.transform.Source;

/**
 * CacheServiceConfigurationParser
 */
public interface CacheServiceConfigurationParser<T extends Service> {

  Source getXmlSchema() throws IOException;

  URI getNamespace();

  ServiceConfiguration<T, ?> parseServiceConfiguration(Element fragment, ClassLoader classLoader);

  Class<T> getServiceType();

  Element unparseServiceConfiguration(ServiceConfiguration<T, ?> serviceConfiguration);
}
