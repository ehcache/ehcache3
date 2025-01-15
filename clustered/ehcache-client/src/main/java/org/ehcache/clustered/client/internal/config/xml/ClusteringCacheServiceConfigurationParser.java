/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
package org.ehcache.clustered.client.internal.config.xml;

import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.xml.CacheServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Provides parsing support for the {@code <service>} elements representing a {@link ClusteredStore.Provider ClusteringService}.
 */
@Component
public class ClusteringCacheServiceConfigurationParser extends ClusteringParser<ClusteredStoreConfiguration> implements CacheServiceConfigurationParser<ClusteredStore.Provider, ClusteredStoreConfiguration> {

  public static final String CLUSTERED_STORE_ELEMENT = "clustered-store";
  public static final String CONSISTENCY_ATTRIBUTE = "consistency";

  @Override
  public ClusteredStoreConfiguration parse(Element fragment, ClassLoader classLoader) {
    if (CLUSTERED_STORE_ELEMENT.equals(fragment.getLocalName())) {
      Attr consistency = fragment.getAttributeNode(CONSISTENCY_ATTRIBUTE);
      if (consistency == null) {
        return ClusteredStoreConfigurationBuilder.withConsistency(Consistency.EVENTUAL).build();
      } else {
        return ClusteredStoreConfigurationBuilder.withConsistency(Consistency.valueOf(consistency.getValue().toUpperCase())).build();
      }
    } else {
      throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }

  @Override
  public Class<ClusteredStore.Provider> getServiceType() {
    return ClusteredStore.Provider.class;
  }

  @Override
  public Element safeUnparse(Document doc, ClusteredStoreConfiguration clusteredStoreConfiguration) {
    Consistency consistency = clusteredStoreConfiguration.getConsistency();
    Element rootElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTERED_STORE_ELEMENT);
    rootElement.setAttribute(CONSISTENCY_ATTRIBUTE, consistency.name().toLowerCase());
    return rootElement;
  }

}
