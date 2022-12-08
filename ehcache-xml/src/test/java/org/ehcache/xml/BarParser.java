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
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

/**
 * BarParser
 */
public class BarParser implements CacheManagerServiceConfigurationParser<Service> {

  private static final URI NAMESPACE = URI.create("http://www.example.com/bar");
  private static final URL XML_SCHEMA = FooParser.class.getResource("/configs/bar.xsd");

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceCreationConfiguration<Service, ?> parseServiceCreationConfiguration(Element fragment, ClassLoader classLoader) {
    return new BarConfiguration();
  }

  @Override
  public Class<Service> getServiceType() {
    return Service.class;
  }

  @Override
  public Element unparseServiceCreationConfiguration(ServiceCreationConfiguration<Service, ?> serviceCreationConfiguration) {
    try {
      Document document = DomUtil.createAndGetDocumentBuilder().newDocument();
      return document.createElementNS(NAMESPACE.toString(), "bar:bar");
    } catch (SAXException | ParserConfigurationException | IOException e) {
      throw new XmlConfigurationException(e);
    }
  }
}
