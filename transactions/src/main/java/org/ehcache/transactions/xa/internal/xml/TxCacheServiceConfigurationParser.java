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

package org.ehcache.transactions.xa.internal.xml;

import org.ehcache.xml.BaseConfigParser;
import org.ehcache.xml.CacheServiceConfigurationParser;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.transactions.xa.internal.XAStore;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.net.URI;
import java.net.URL;

import static org.ehcache.transactions.xa.internal.xml.TxCacheManagerServiceConfigurationParser.TRANSACTION_NAMESPACE_PREFIX;

/**
 * @author Ludovic Orban
 */
@Component
public class TxCacheServiceConfigurationParser extends BaseConfigParser<XAStoreConfiguration> implements CacheServiceConfigurationParser<XAStore.Provider> {

  private static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/tx");
  private static final URL XML_SCHEMA = TxCacheManagerServiceConfigurationParser.class.getResource("/ehcache-tx-ext.xsd");
  private static final String STORE_ELEMENT_NAME = "xa-store";
  private static final String UNIQUE_RESOURCE_NAME = "unique-XAResource-id";

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceConfiguration<XAStore.Provider> parseServiceConfiguration(Element fragment, ClassLoader classLoader) {
    String localName = fragment.getLocalName();
    if ("xa-store".equals(localName)) {
      String uniqueXAResourceId = fragment.getAttribute("unique-XAResource-id");
      return new XAStoreConfiguration(uniqueXAResourceId);
    } else {
      throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
          fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }

  @Override
  public Class<XAStore.Provider> getServiceType() {
    return XAStore.Provider.class;
  }

  @Override
  public Element unparseServiceConfiguration(ServiceConfiguration<XAStore.Provider> serviceConfiguration) {
    return unparseConfig(serviceConfiguration);
  }

  @Override
  protected Element createRootElement(Document doc, XAStoreConfiguration storeConfiguration) {
    Element rootElement = doc.createElementNS(NAMESPACE.toString(), TRANSACTION_NAMESPACE_PREFIX  + STORE_ELEMENT_NAME);
    rootElement.setAttribute(UNIQUE_RESOURCE_NAME, storeConfiguration.getUniqueXAResourceId());
    return rootElement;
  }

}
