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
package org.ehcache.xml;

import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static java.util.Objects.requireNonNull;
import static org.ehcache.xml.XmlUtil.newSchema;

public class DomUtil {

  private static final URL CORE_SCHEMA_URL = requireNonNull(XmlConfiguration.class.getResource("/ehcache-core.xsd"));

  private static DocumentBuilder createAndGetDocumentBuilder(Collection<Supplier<Source>> additionalSchema) throws SAXException, ParserConfigurationException, IOException {
    List<Source> schemaSources = new ArrayList<>(additionalSchema.size() + 1);
    schemaSources.add(new StreamSource(CORE_SCHEMA_URL.openStream()));
    schemaSources.addAll(additionalSchema.stream().map(Supplier::get).collect(Collectors.toList()));

    DocumentBuilderFactory factory = createAndGetFactory(schemaSources);
    DocumentBuilder documentBuilder = factory.newDocumentBuilder();
    documentBuilder.setErrorHandler(new TransformationErrorHandler());
    return documentBuilder;
  }

  private static DocumentBuilderFactory createAndGetFactory(List<Source> schemaSources) throws SAXException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);
    factory.setSchema(newSchema(schemaSources.toArray(new Source[0])));
    return factory;
  }

  public static Document createDocumentRoot(Collection<Supplier<Source>> schemaSource) throws IOException, SAXException, ParserConfigurationException {
    return createAndGetDocumentBuilder(schemaSource).newDocument();
  }

  static class TransformationErrorHandler implements ErrorHandler {

    @Override
    public void warning(SAXParseException exception) throws SAXException {
      throw exception;
    }

    @Override
    public void error(SAXParseException exception) throws SAXException {
      throw exception;
    }

    @Override
    public void fatalError(SAXParseException exception) throws SAXException {
      throw exception;
    }
  }
}
