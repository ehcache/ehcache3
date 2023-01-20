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

import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerLookup;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Element;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.net.URI;
import java.net.URL;

/**
 * @author Ludovic Orban
 */
public class TxCacheManagerServiceConfigurationParser implements CacheManagerServiceConfigurationParser<TransactionManagerProvider> {

  private static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/tx");
  private static final URL XML_SCHEMA = TxCacheManagerServiceConfigurationParser.class.getResource("/ehcache-tx-ext.xsd");

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceCreationConfiguration<TransactionManagerProvider> parseServiceCreationConfiguration(Element fragment) {
    String localName = fragment.getLocalName();
    if ("jta-tm".equals(localName)) {
      String transactionManagerProviderConfigurationClassName = fragment.getAttribute("transaction-manager-lookup-class");
      try {
        ClassLoader defaultClassLoader = ClassLoading.getDefaultClassLoader();
        Class<?> aClass = Class.forName(transactionManagerProviderConfigurationClassName, true, defaultClassLoader);
        return new LookupTransactionManagerProviderConfiguration((Class<? extends TransactionManagerLookup>) aClass);
      } catch (Exception e) {
        throw new XmlConfigurationException("Error configuring XA transaction manager", e);
      }
    } else {
      throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
          fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }
}
