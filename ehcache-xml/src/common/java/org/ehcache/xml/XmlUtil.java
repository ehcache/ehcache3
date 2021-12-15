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
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXParseException;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.regex.Pattern.quote;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;

public class XmlUtil {

  private static final URL CORE_SCHEMA_URL = XmlUtil.class.getResource("/ehcache-core.xsd");

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

  public static Schema newSchema(Source... schemas) throws SAXException {
    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    try {
      /*
       * Our schema is accidentally not XSD 1.1 compatible. Since Saxon incorrectly (imho) defaults to XSD 1.1 for
       * `XMLConstants.W3C_XML_SCHEMA_NS_URI` we force it back to 1.0.
       */
      schemaFactory.setProperty("http://saxon.sf.net/feature/xsd-version", "1.0");
    } catch (SAXNotRecognizedException e) {
      //not saxon
    }
    schemaFactory.setErrorHandler(new FatalErrorHandler());
    return schemaFactory.newSchema(schemas);
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

  public static class FatalErrorHandler implements ErrorHandler {

    private static final Collection<Pattern> ABSTRACT_TYPE_FAILURES;
    static {
      List<QName> abstractTypes = asList(
        new QName("http://www.ehcache.org/v3", "service-creation-configuration"),
        new QName("http://www.ehcache.org/v3", "service-configuration"),
        new QName("http://www.ehcache.org/v3", "resource"));

      ABSTRACT_TYPE_FAILURES = asList(
        //Xerces
        abstractTypes.stream().map(element -> quote(format("\"%s\":%s", element.getNamespaceURI(), element.getLocalPart())))
          .collect(collectingAndThen(joining("|", "^\\Qcvc-complex-type.2.4.a\\E.*'\\{.*(?:", ").*\\}'.*$"), Pattern::compile)),
        //Saxon
        abstractTypes.stream().map(element -> quote(element.getLocalPart()))
          .collect(collectingAndThen(joining("|", "^.*\\QThe content model does not allow element\\E.*(?:", ").*"), Pattern::compile)));
    }

    @Override
    public void warning(SAXParseException exception) throws SAXException {
      fatalError(exception);
    }

    @Override
    public void error(SAXParseException exception) throws SAXException {
      fatalError(exception);
    }

    @Override
    public void fatalError(SAXParseException exception) throws SAXException {
      if (ABSTRACT_TYPE_FAILURES.stream().anyMatch(pattern -> pattern.matcher(exception.getMessage()).matches())) {
        throw new XmlConfigurationException(
          "Cannot confirm XML sub-type correctness. You might be missing client side libraries.", exception);
      } else {
        throw exception;
      }
    }
  }
}
