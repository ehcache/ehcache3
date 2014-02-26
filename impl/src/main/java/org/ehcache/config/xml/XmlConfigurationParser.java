/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.config.xml;

import java.io.IOException;
import java.net.URI;
import javax.xml.transform.Source;
import org.ehcache.spi.Service;
import org.ehcache.spi.ServiceConfiguration;
import org.w3c.dom.Element;

/**
 *
 * @author cdennis
 */
public interface XmlConfigurationParser<T extends Service> {
  
  Source getXmlSchema() throws IOException;
  
  URI getNamespace();
  
  ServiceConfiguration<T> parse(Element fragment);
}
