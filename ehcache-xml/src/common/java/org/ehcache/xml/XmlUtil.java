/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXParseException;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.regex.Pattern.quote;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static org.ehcache.core.util.ClassLoading.servicesOfType;

public class XmlUtil {

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

  public static <T> List<T> mergePartialOrderings(Collection<List<T>> orderings) {
    Collection<List<T>> workingCopy = orderings.stream().map(ArrayList::new).collect(toCollection(ArrayList::new));
    List<T> fullOrdering = new ArrayList<>();

    while (!workingCopy.isEmpty()) {
      boolean progress = false;
      for (Iterator<List<T>> partials = workingCopy.iterator(); partials.hasNext(); ) {
        List<T> partial = partials.next();

        if (partial.isEmpty()) {
          progress = true;
          partials.remove();
        } else {
          Supplier<Stream<List<T>>> otherOrderings = () -> workingCopy.stream().filter(o -> o != partial);

          for (Iterator<T> it = partial.iterator(); it.hasNext(); it.remove()) {
            T next = it.next();

            if (otherOrderings.get().anyMatch(o -> o.indexOf(next) > 0)) {
              break;
            } else {
              progress = true;
              fullOrdering.add(next);
              otherOrderings.get().forEach(o -> o.remove(next));
            }
          }
        }
      }
      if (!progress) {
        throw new IllegalArgumentException("Incompatible partial orderings: " + orderings);
      }
    }

    return fullOrdering;
  }

  public static <T extends Parser<?>> Iterable<T> namespaceUniqueParsersOfType(Class<T> parserType) {
    List<T> parsers = new ArrayList<>();
    for (T parser : servicesOfType(parserType)) {
      if (allowedInParserSet(parsers, parser)) {
        parsers.add(parser);
      }
    }
    return unmodifiableList(parsers);
  }

  private static <T extends Parser<?>> boolean allowedInParserSet(List<T> parsers, T parser) {
    Set<URI> parserTargetNamespaces = parser.getTargetNamespaces();
    for (Iterator<T> it = parsers.iterator(); it.hasNext(); ) {
      T other = it.next();
      Set<URI> otherTargetNamespaces = other.getTargetNamespaces();
      if (parserTargetNamespaces.equals(otherTargetNamespaces)) {
        throw new IllegalArgumentException("Parsers competing for identical namespace set: " + parser + " :: " + other);
      } else if (parserTargetNamespaces.containsAll(otherTargetNamespaces)) {
        it.remove();
      } else if (otherTargetNamespaces.containsAll(parserTargetNamespaces)) {
        return false;
      } else {
        Set<URI> intersection = new HashSet<>(parserTargetNamespaces);
        intersection.retainAll(otherTargetNamespaces);
        if (!intersection.isEmpty()) {
          throw new IllegalArgumentException("Parsers competing for namespace set: " + intersection + " (neither dominates the other): " + parser + " :: " + other);
        }
      }
    }
    return true;
  }

  private static final String EXTERNAL_IDENTIFIER_ATTRIBUTE_NAME = "external-identifier";

  public static Document stampExternalConfigurations(Document document) {
    String mainNamespace = document.getDocumentElement().getNamespaceURI();
    NodeList elements = document.getElementsByTagName("*");
    for (int i = 0, identifier = 0; i < elements.getLength(); i++) {
      Element element = (Element) elements.item(i);
      if (!element.getNamespaceURI().equals(mainNamespace)) {
        element.setAttributeNS(mainNamespace, EXTERNAL_IDENTIFIER_ATTRIBUTE_NAME, valueOf(identifier++));
      }
    }
    return document;
  }

  private static Element cleanExternalConfigurations(Element fragment) {
    String mainNamespace = fragment.getOwnerDocument().getDocumentElement().getNamespaceURI();
    NodeList elements = fragment.getElementsByTagName("*");
    for (int i = 0; i < elements.getLength(); i++) {
      Element element = (Element) elements.item(i);
      element.removeAttributeNS(mainNamespace, EXTERNAL_IDENTIFIER_ATTRIBUTE_NAME);
    }
    return fragment;
  }

  public static Element findMatchingNodeInDocument(Document document, Element lookup) {
    String mainNamespace = document.getDocumentElement().getNamespaceURI();
    String identifier = lookup.getAttributeNS(mainNamespace, EXTERNAL_IDENTIFIER_ATTRIBUTE_NAME);
    if (identifier.isEmpty()) {
      throw new IllegalArgumentException("Cannot lookup unstamped element: " + lookup);
    } else {
      NodeList elements = document.getElementsByTagName("*");

      for (int i = 0; i < elements.getLength(); i++) {
        Element element = (Element) elements.item(i);
        if (identifier.equals(element.getAttributeNS(mainNamespace, EXTERNAL_IDENTIFIER_ATTRIBUTE_NAME))) {
          if (lookup.getLocalName().equals(element.getLocalName()) && lookup.getNamespaceURI().equals(element.getNamespaceURI())) {
            return cleanExternalConfigurations(element);
          } else {
            throw new IllegalStateException("Lookup of: " + lookup + " found mismatched element: " + element);
          }
        }
      }
      throw new IllegalArgumentException("No element found for: " + lookup);
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
