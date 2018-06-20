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

import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Objects;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;

/**
 * BaseConfigParser - Base class providing functionality for translating service configurations to corresponding xml
 * document.
 */
public abstract class BaseConfigParser<T> {
  private final Class<T> typeParameterClass;

  @SuppressWarnings("unchecked")
  public BaseConfigParser() {
    typeParameterClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
  }

  public BaseConfigParser(Class<T> type) {
    this.typeParameterClass = type;
  }

  private T validateConfig(Object config) {
    Objects.requireNonNull(config, "Configuration must not be null.");
    try {
      return typeParameterClass.cast(config);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Invalid configuration parameter passed.", e);
    }
  }

  private Document createDocument() {
    try {
      return DomUtil.createDocumentRoot(getXmlSchema());
    } catch (SAXException | ParserConfigurationException | IOException e) {
      throw new XmlConfigurationException(e);
    }
  }

  protected Element unparseConfig(Object config) {
    T mainConfig = validateConfig(config);
    Document doc = createDocument();
    Element rootElement = createRootElement(doc, mainConfig);
    return rootElement;
  }

  protected abstract Element createRootElement(Document doc, T config);

  protected abstract Source getXmlSchema() throws IOException;
}
