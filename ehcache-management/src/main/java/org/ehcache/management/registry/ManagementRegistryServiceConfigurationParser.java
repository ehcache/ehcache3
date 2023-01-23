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
package org.ehcache.management.registry;

import org.ehcache.management.ManagementRegistryService;
import org.ehcache.xml.BaseConfigParser;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.net.URI;

import static java.util.Collections.singletonMap;
import static org.ehcache.xml.ParsingUtil.parsePropertyOrString;

public class ManagementRegistryServiceConfigurationParser extends BaseConfigParser<DefaultManagementRegistryConfiguration>
  implements CacheManagerServiceConfigurationParser<ManagementRegistryService, DefaultManagementRegistryConfiguration> {

  private static final String NAMESPACE = "http://www.ehcache.org/v3/management";
  private static final String MANAGEMENT_NAMESPACE_PREFIX = "mgm:";
  private static final String MANAGEMENT_ELEMENT_NAME = "management";
  private static final String CACHE_MANAGER_ATTRIBUTE_NAME = "cache-manager-alias";
  private static final String COLLECTOR_EXECUTOR_ATTRIBUTE_NAME = "collector-executor-alias";
  private static final String TAGS_NAME = "tags";
  private static final String TAG_NAME = "tag";

  public ManagementRegistryServiceConfigurationParser() {
    super(singletonMap(URI.create(NAMESPACE), ManagementRegistryServiceConfigurationParser.class.getResource("/ehcache-management-ext.xsd")));
  }

  @Override
  public DefaultManagementRegistryConfiguration parse(Element fragment, ClassLoader classLoader) {
    if (MANAGEMENT_ELEMENT_NAME.equals(fragment.getLocalName())) {
      DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration();

      // ATTR: cache-manager-alias
      if (fragment.hasAttribute(CACHE_MANAGER_ATTRIBUTE_NAME)) {
        registryConfiguration.setCacheManagerAlias(attr(fragment, CACHE_MANAGER_ATTRIBUTE_NAME));
      }

      // ATTR: collector-executor-alias
      if (fragment.hasAttribute(COLLECTOR_EXECUTOR_ATTRIBUTE_NAME)) {
        registryConfiguration.setCollectorExecutorAlias(attr(fragment, COLLECTOR_EXECUTOR_ATTRIBUTE_NAME));
      }

      // tags
      for (Element tags : NodeListIterable.elements(fragment, NAMESPACE, TAGS_NAME)) {
        // tag
        for (Element tag : NodeListIterable.elements(tags, NAMESPACE, TAG_NAME)) {
          String val = parsePropertyOrString(tag.getTextContent());
          if (!val.isEmpty()) {
            registryConfiguration.addTag(val);
          }
        }
      }

      return registryConfiguration;

    } else {
      throw new XmlConfigurationException(String.format(
        "XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }

  private static String attr(Element element, String name) {
    String s = element.getAttribute(name);
    return s == null || s.equals("") ? null : s;
  }

  @Override
  public Class<ManagementRegistryService> getServiceType() {
    return ManagementRegistryService.class;
  }

  @Override
  public Element safeUnparse(Document doc, DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration) {
    Element rootElement = doc.createElementNS(NAMESPACE,MANAGEMENT_NAMESPACE_PREFIX + MANAGEMENT_ELEMENT_NAME);
    rootElement.setAttribute(CACHE_MANAGER_ATTRIBUTE_NAME, defaultManagementRegistryConfiguration.getCacheManagerAlias());
    rootElement.setAttribute(COLLECTOR_EXECUTOR_ATTRIBUTE_NAME, defaultManagementRegistryConfiguration.getCollectorExecutorAlias());
    processManagementTags(doc, rootElement, defaultManagementRegistryConfiguration);
    return rootElement;
  }

  private void processManagementTags(Document doc, Element parent, DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration) {
    if (!defaultManagementRegistryConfiguration.getTags().isEmpty()) {
      Element tagsName = doc.createElementNS(NAMESPACE, MANAGEMENT_NAMESPACE_PREFIX + TAGS_NAME);
      for (String tag : defaultManagementRegistryConfiguration.getTags()) {
        Element tagName = doc.createElementNS(NAMESPACE, MANAGEMENT_NAMESPACE_PREFIX + TAG_NAME);
        tagName.setTextContent(tag);
        tagsName.appendChild(tagName);
      }
      parent.appendChild(tagsName);
    }
  }

}
