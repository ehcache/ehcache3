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
package org.ehcache.xml.ss;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.ehcache.core.util.ClassLoading.delegationChain;

public class SimpleServiceConfigurationParser implements CacheManagerServiceConfigurationParser<SimpleServiceProvider> {

  private static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/ss");
  private static final URL XML_SCHEMA = SimpleServiceConfigurationParser.class.getResource("/ehcache-simple-service-ext.xsd");

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceCreationConfiguration<SimpleServiceProvider, ?> parseServiceCreationConfiguration(Element fragment, ClassLoader classLoader) {
    String localName = fragment.getLocalName();
    if ("instance".equals(localName)) {
      String serviceClassName = fragment.getAttribute("class");
      try {
        Class<?> aClass = Class.forName(serviceClassName, true, delegationChain(
          () -> Thread.currentThread().getContextClassLoader(),
          getClass().getClassLoader(),
          classLoader
        ));
        Class<? extends Service> clazz = uncheckedCast(aClass);

        List<String> unparsedArgs = new ArrayList<>();
        NodeList childNodes = fragment.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
          Node node = childNodes.item(i);
          if ("arg".equals(node.getLocalName())) {
            String value = ((Element) node).getAttribute("value");
            unparsedArgs.add(value);
          } // TODO else throw?
        }

        return new SimpleServiceConfiguration(clazz, unparsedArgs);
      } catch (Exception e) {
        throw new XmlConfigurationException("Error configuring simple service", e);
      }
    } else {
      throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }

  /**
   * Perform a (warning suppressed) unchecked cast to an inferred type {@code U}.
   */
  @SuppressWarnings("unchecked")
  private static <U> U uncheckedCast(Object o) {
    return (U) o;
  }

  @Override
  public Class<SimpleServiceProvider> getServiceType() {
    return SimpleServiceProvider.class;
  }

  @Override
  public Element unparseServiceCreationConfiguration(ServiceCreationConfiguration<SimpleServiceProvider, ?> serviceCreationConfiguration) {
    return null; // TODO implement
  }
}
