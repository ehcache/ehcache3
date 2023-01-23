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
import org.ehcache.transactions.xa.internal.XAStore;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.net.URI;

import static java.util.Collections.singletonMap;
import static org.ehcache.transactions.xa.internal.xml.TxCacheManagerServiceConfigurationParser.TRANSACTION_NAMESPACE_PREFIX;
import static org.ehcache.xml.ParsingUtil.parsePropertyOrString;

/**
 * @author Ludovic Orban
 */
@Component
public class TxCacheServiceConfigurationParser extends BaseConfigParser<XAStoreConfiguration> implements CacheServiceConfigurationParser<XAStore.Provider, XAStoreConfiguration> {

  private static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/tx");
  private static final String STORE_ELEMENT_NAME = "xa-store";
  private static final String UNIQUE_RESOURCE_NAME = "unique-XAResource-id";

  public TxCacheServiceConfigurationParser() {
    super(singletonMap(NAMESPACE, TxCacheManagerServiceConfigurationParser.class.getResource("/ehcache-tx-ext.xsd")));
  }

  @Override
  public XAStoreConfiguration parse(Element fragment, ClassLoader classLoader) {
    String localName = fragment.getLocalName();
    if ("xa-store".equals(localName)) {
      String uniqueXAResourceId = parsePropertyOrString(fragment.getAttribute("unique-XAResource-id"));
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
  public Element safeUnparse(Document doc, XAStoreConfiguration storeConfiguration) {
    Element rootElement = doc.createElementNS(NAMESPACE.toString(), TRANSACTION_NAMESPACE_PREFIX  + STORE_ELEMENT_NAME);
    rootElement.setAttribute(UNIQUE_RESOURCE_NAME, storeConfiguration.getUniqueXAResourceId());
    return rootElement;
  }

}
