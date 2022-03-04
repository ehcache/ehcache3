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
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

/**
 * BarParser
 */
public class BarParser implements CacheManagerServiceConfigurationParser<Service, ServiceCreationConfiguration<Service, ?>> {

  private static final String NAMESPACE = "http://www.example.com/bar";

  @Override
  public Map<URI, Supplier<Source>> getSchema() {
    return Collections.singletonMap(URI.create(NAMESPACE), () -> new StreamSource(getClass().getResourceAsStream("/configs/bar.xsd")));
  }

  @Override
  public ServiceCreationConfiguration<Service, ?> parse(Element fragment, ClassLoader classLoader) {
    return new BarConfiguration();
  }

  @Override
  public Class<Service> getServiceType() {
    return Service.class;
  }

  @Override
  public Element unparse(Document target, ServiceCreationConfiguration<Service, ?> serviceCreationConfiguration) {
    return target.createElementNS(NAMESPACE, "bar:bar");
  }
}
