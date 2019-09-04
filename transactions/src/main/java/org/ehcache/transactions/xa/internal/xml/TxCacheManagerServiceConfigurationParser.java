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
import org.ehcache.xml.BaseConfigParser;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import static org.ehcache.core.util.ClassLoading.delegationChain;
import static org.ehcache.transactions.xa.internal.TypeUtil.uncheckedCast;

/**
 * @author Ludovic Orban
 */
@Component
public class TxCacheManagerServiceConfigurationParser extends BaseConfigParser<LookupTransactionManagerProviderConfiguration> implements CacheManagerServiceConfigurationParser<TransactionManagerProvider> {
  private static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/tx");
  private static final URL XML_SCHEMA = TxCacheManagerServiceConfigurationParser.class.getResource("/ehcache-tx-ext.xsd");
  public static final String TRANSACTION_NAMESPACE_PREFIX = "tx:";
  private static final String TRANSACTION_ELEMENT_NAME = "jta-tm";
  private static final String TRANSACTION_LOOKUP_CLASS = "transaction-manager-lookup-class";

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceCreationConfiguration<TransactionManagerProvider, ?> parseServiceCreationConfiguration(Element fragment, ClassLoader classLoader) {
    String localName = fragment.getLocalName();
    if ("jta-tm".equals(localName)) {
      String transactionManagerProviderConfigurationClassName = fragment.getAttribute("transaction-manager-lookup-class");
      try {
        Class<?> aClass = Class.forName(transactionManagerProviderConfigurationClassName, true, delegationChain(
          () -> Thread.currentThread().getContextClassLoader(),
          getClass().getClassLoader(),
          classLoader
        ));
        Class<? extends TransactionManagerLookup> clazz = uncheckedCast(aClass);
        return new LookupTransactionManagerProviderConfiguration(clazz);
      } catch (Exception e) {
        throw new XmlConfigurationException("Error configuring XA transaction manager", e);
      }
    } else {
      throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
          fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }

  @Override
  public Class<TransactionManagerProvider> getServiceType() {
    return TransactionManagerProvider.class;
  }

  @Override
  public Element unparseServiceCreationConfiguration(ServiceCreationConfiguration<TransactionManagerProvider, ?> serviceCreationConfiguration) {
    return unparseConfig(serviceCreationConfiguration);
  }

  @Override
  protected Element createRootElement(Document doc, LookupTransactionManagerProviderConfiguration lookupTransactionManagerProviderConfiguration) {
    Element rootElement = doc.createElementNS(NAMESPACE.toString(), TRANSACTION_NAMESPACE_PREFIX + TRANSACTION_ELEMENT_NAME);
    rootElement.setAttribute(TRANSACTION_LOOKUP_CLASS, lookupTransactionManagerProviderConfiguration.getTransactionManagerLookup()
      .getName());
    return rootElement;
  }

}
