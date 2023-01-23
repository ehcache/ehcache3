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

import jakarta.transaction.TransactionManager;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerLookup;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.ehcache.xml.BaseConfigParser;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.net.URI;

import static java.util.Collections.singletonMap;
import static org.ehcache.core.util.ClassLoading.delegationChain;
import static org.ehcache.transactions.xa.internal.TypeUtil.uncheckedCast;
import static org.ehcache.xml.ParsingUtil.parsePropertyOrString;

/**
 * @author Ludovic Orban
 */
@Component
public class TxCacheManagerServiceConfigurationParser extends BaseConfigParser<LookupTransactionManagerProviderConfiguration>
  implements CacheManagerServiceConfigurationParser<TransactionManagerProvider<TransactionManager>, LookupTransactionManagerProviderConfiguration> {
  private static final String NAMESPACE = "http://www.ehcache.org/v3/tx";
  public static final String TRANSACTION_NAMESPACE_PREFIX = "tx:";
  private static final String TRANSACTION_ELEMENT_NAME = "jta-tm";
  private static final String TRANSACTION_LOOKUP_CLASS = "transaction-manager-lookup-class";

  public TxCacheManagerServiceConfigurationParser() {
    super(singletonMap(URI.create(NAMESPACE), TxCacheManagerServiceConfigurationParser.class.getResource("/ehcache-tx-ext.xsd")));
  }

  @Override
  public LookupTransactionManagerProviderConfiguration parse(Element fragment, ClassLoader classLoader) {
    String localName = fragment.getLocalName();
    if ("jta-tm".equals(localName)) {
      String transactionManagerProviderConfigurationClassName = parsePropertyOrString(fragment.getAttribute("transaction-manager-lookup-class"));
      try {
        Class<?> aClass = Class.forName(transactionManagerProviderConfigurationClassName, true, delegationChain(
          () -> Thread.currentThread().getContextClassLoader(),
          getClass().getClassLoader(),
          classLoader
        ));
        Class<? extends TransactionManagerLookup<TransactionManager>> clazz = uncheckedCast(aClass);
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
  public Class<TransactionManagerProvider<TransactionManager>> getServiceType() {
    return uncheckedCast(TransactionManagerProvider.class);
  }

  @Override
  public Element safeUnparse(Document doc, LookupTransactionManagerProviderConfiguration lookupTransactionManagerProviderConfiguration) {
    Element rootElement = doc.createElementNS(NAMESPACE, TRANSACTION_NAMESPACE_PREFIX + TRANSACTION_ELEMENT_NAME);
    rootElement.setAttribute(TRANSACTION_LOOKUP_CLASS, lookupTransactionManagerProviderConfiguration.getTransactionManagerLookup().getName());
    return rootElement;
  }

}
