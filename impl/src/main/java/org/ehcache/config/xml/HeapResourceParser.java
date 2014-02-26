/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config.xml;

import org.ehcache.internal.HeapResource;
import org.ehcache.internal.HeapResourceCacheConfiguration;
import org.ehcache.spi.ServiceConfiguration;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

/**
 * @author Alex Snaps
 */
public class HeapResourceParser implements XmlConfigurationParser<HeapResource> {

  private static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/cachingtier");
  private static final URL XML_SCHEMA = HeapResourceParser.class.getResource("/ehcache-cachingtier.xsd");

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceConfiguration<HeapResource> parse(final Element heapConfig) {
    final long maxOnHeapEntryCount = Long.parseLong(heapConfig.getAttribute("size"));
    return new HeapResourceCacheConfiguration(maxOnHeapEntryCount);
  }
}
