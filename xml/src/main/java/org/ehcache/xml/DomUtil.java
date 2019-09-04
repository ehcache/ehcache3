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

import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

public class DomUtil {

  private static final SchemaFactory XSD_SCHEMA_FACTORY = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
  private static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");

  private static Schema newSchema(Source[] schemas) throws SAXException {
    synchronized (XSD_SCHEMA_FACTORY) {
      return XSD_SCHEMA_FACTORY.newSchema(schemas);
    }
  }

  public static DocumentBuilder createAndGetDocumentBuilder(Collection<Source> schemaSources) throws SAXException, ParserConfigurationException {
    DocumentBuilderFactory factory = createAndGetFactory(schemaSources);
    DocumentBuilder documentBuilder = factory.newDocumentBuilder();
    documentBuilder.setErrorHandler(new TransformationErrorHandler());
    return documentBuilder;
  }

  public static DocumentBuilder createAndGetDocumentBuilder(Source schemaSource) throws SAXException, ParserConfigurationException, IOException {
    List<Source> schemaSources = new ArrayList<>(2);
    schemaSources.add(new StreamSource(CORE_SCHEMA_URL.openStream()));
    schemaSources.add(schemaSource);
    return createAndGetDocumentBuilder(schemaSources);
  }

  public static DocumentBuilder createAndGetDocumentBuilder() throws SAXException, ParserConfigurationException, IOException {
    return createAndGetDocumentBuilder(new StreamSource(CORE_SCHEMA_URL.openStream()));
  }

  private static DocumentBuilderFactory createAndGetFactory(Collection<Source> schemaSources) throws SAXException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);
    factory.setSchema(newSchema(schemaSources.toArray(new Source[schemaSources.size()])));
    return factory;
  }

  public static Document createDocumentRoot(Source schemaSource) throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilder domBuilder = createAndGetDocumentBuilder(schemaSource);
    Document doc = domBuilder.newDocument();
    return doc;
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
