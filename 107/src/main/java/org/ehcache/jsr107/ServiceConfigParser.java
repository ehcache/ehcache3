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

package org.ehcache.jsr107;

import org.ehcache.config.Jsr107Configuration;
import org.ehcache.config.xml.XmlConfigurationParser;
import org.ehcache.spi.service.ServiceConfiguration;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

/**
 * @author Alex Snaps
 */
public class ServiceConfigParser implements XmlConfigurationParser<Jsr107Service> {

  private static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/jsr107");
  private static final URL XML_SCHEMA = ServiceConfigParser.class.getResource("/ehcache-107ext.xsd");

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceConfiguration<Jsr107Service> parse(final Element fragment) {
    final String defaultTemplate = fragment.getAttribute("default-template");
    final HashMap<String, String> templates = new HashMap<String, String>();
    final NodeList childNodes = fragment.getChildNodes();
    for(int i = 0; i < childNodes.getLength(); i++) {
      final Element item = (Element)childNodes.item(i);
      templates.put(item.getAttribute("name"), item.getAttribute("template"));
    }

    return new Jsr107Configuration(defaultTemplate, templates);
  }
}
