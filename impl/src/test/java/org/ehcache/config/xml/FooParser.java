/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config.xml;

import org.ehcache.spi.Service;
import org.ehcache.spi.ServiceConfiguration;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

/**
 *
 * @author cdennis
 */
public class FooParser implements XmlConfigurationParser<Service> {

  private static final URI NAMESPACE = URI.create("http://www.example.com/foo");
  private static final URL XML_SCHEMA = FooParser.class.getResource("/configs/foo.xsd");
  
  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public ServiceConfiguration<Service> parse(Element fragment) {
    return new FooConfiguration();
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }
  
}
