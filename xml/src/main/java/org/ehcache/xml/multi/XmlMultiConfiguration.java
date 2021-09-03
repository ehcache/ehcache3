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

package org.ehcache.xml.multi;

import org.ehcache.config.Configuration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.multi.model.Configurations;
import org.ehcache.xml.multi.model.ObjectFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSchema;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.ehcache.xml.ConfigurationParser.discoverSchema;
import static org.ehcache.xml.ConfigurationParser.documentBuilder;
import static org.ehcache.xml.ConfigurationParser.documentToText;
import static org.ehcache.xml.ConfigurationParser.urlToText;
import static org.ehcache.xml.XmlConfiguration.CORE_SCHEMA_URL;

/**
 * A collection of multiple Ehcache configurations.
 */
public class XmlMultiConfiguration {

  private static final URL MULTI_SCHEMA_URL = XmlMultiConfiguration.class.getResource("/ehcache-multi.xsd");
  private static final QName MULTI_SCHEMA_ROOT_NAME = new QName(
    Configurations.class.getPackage().getAnnotation(XmlSchema.class).namespace(),
    Configurations.class.getAnnotation(XmlRootElement.class).name());

  private final Map<String, Config> configurations;

  private final Document document;
  private final String renderedDocument;

  @SuppressWarnings("unchecked")
  private XmlMultiConfiguration(URL url, BiFunction<String, Document, XmlConfiguration> configParser) throws XmlConfigurationException {
    try {
      Schema schema = discoverSchema(new StreamSource(CORE_SCHEMA_URL.openStream()), new StreamSource(MULTI_SCHEMA_URL.openStream()));
      DocumentBuilder domBuilder = documentBuilder(schema);
      this.document = domBuilder.parse(url.toExternalForm());
      this.renderedDocument = urlToText(url, document.getInputEncoding());

      Element rootElement = document.getDocumentElement();

      QName rootName = new QName(rootElement.getNamespaceURI(), rootElement.getLocalName());
      if (!MULTI_SCHEMA_ROOT_NAME.equals(rootName)) {
        throw new XmlConfigurationException("Expecting " + MULTI_SCHEMA_ROOT_NAME + " element; found " + rootName);
      }

      JAXBContext jaxbContext = JAXBContext.newInstance(Configurations.class);
      Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
      Configurations value = unmarshaller.unmarshal(rootElement, Configurations.class).getValue();

      this.configurations = value.getConfiguration().stream().collect(toMap(Configurations.Configuration::getIdentity, c -> {

        Element configuration = c.getConfig();
        if (configuration != null) {
          Document configDoc = domBuilder.newDocument();
          configDoc.appendChild(configDoc.importNode(configuration, true));
          return new SingleConfig(configParser.apply(c.getIdentity(), configDoc));
        } else {
          return new VariantConfig(c.getVariant().stream()
            .collect(toMap(Configurations.Configuration.Variant::getType, v -> {
              Document configDoc = domBuilder.newDocument();
              configDoc.appendChild(configDoc.importNode(v.getConfig(), true));
              return configParser.apply(c.getIdentity(), configDoc);
            })));
        }
      }));
    } catch (ParserConfigurationException | SAXException | IOException | JAXBException e) {
      throw new XmlConfigurationException(e);
    }
  }

  private XmlMultiConfiguration(Map<String, Config> configurations) {
    try {
      Schema schema = discoverSchema(new StreamSource(CORE_SCHEMA_URL.openStream()), new StreamSource(MULTI_SCHEMA_URL.openStream()));

      this.configurations = configurations;

      ObjectFactory objectFactory = new ObjectFactory();
      Configurations jaxb = objectFactory.createConfigurations().withConfiguration(configurations.entrySet().stream().map(
        entry -> entry.getValue().unparse(objectFactory, objectFactory.createConfigurationsConfiguration().withIdentity(entry.getKey()))).collect(toList()));

      JAXBContext jaxbContext = JAXBContext.newInstance(Configurations.class);
      Marshaller marshaller = jaxbContext.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
      marshaller.setSchema(schema);

      this.document = documentBuilder(schema).newDocument();
      marshaller.marshal(jaxb, document);
      this.renderedDocument = documentToText(document);
    } catch (JAXBException | IOException | TransformerException | ParserConfigurationException | SAXException e) {
      throw new XmlConfigurationException(e);
    }
  }

  /**
   * Retrieve the singular configuration for {@code identity}.
   * <p>
   * If the given identity is associated with multiple variant configurations then an {@code IllegalStateException} will
   * be thrown. In this case the {@link #configuration(String, String)} method must be used to select a specific
   * variant.
   *
   * @param identity identity to retrieve
   * @return the configuration for the given identity; {@code null} if the identity is not in this configuration
   * @throws IllegalArgumentException if the identity is associated with multiple variant configurations
   */
  public Configuration configuration(String identity) throws IllegalArgumentException {
    Config variants = configurations.get(identity);
    if (variants == null) {
      return null;
    } else {
      return variants.configuration();
    }
  }

  /**
   * Retrieve the singular configuration for {@code identity} and {@code variant}.
   * <p>
   * If the given identity is associated only with a singular configuration then that configuration will be returned for
   * all variants.
   *
   * @param identity identity to retrieve
   * @param variant variant to retrieve
   * @return the configuration for the given identity; {@code null} if the identity is not in this configuration
   * @throws IllegalArgumentException if the given variant does not exist
   */
  public Configuration configuration(String identity, String variant) {
    Config config = configurations.get(identity);
    if (config == null) {
      return null;
    } else {
      return config.configuration(variant);
    }
  }

  /**
   * Return the set of variants defined for the given configuration.
   * <p>
   * If the given identity does not exist then an {@code IllegalArgumentException} is thrown. If the given identity is
   * not variant-ed then an empty set is returned.
   *
   * @return the set of variants; possibly empty.
   * @throws IllegalArgumentException if the identity does not exist
   */
  public Set<String> variants(String identity) throws IllegalArgumentException {
    Config config = configurations.get(identity);
    if (config == null) {
      throw new IllegalArgumentException("Identity " + identity + " does not exist.");
    } else {
      return config.variants();
    }
  }

  /**
   * Return the set of identities defined in this multi-configuration.
   *
   * @return the defined identity set
   */
  public Set<String> identities() {
    return unmodifiableSet(configurations.keySet());
  }

  /**
   * Return this configuration as an XML {@link org.w3c.dom.Document}.
   *
   * @return configuration XML DOM.
   */
  public Document asDocument() {
    return document;
  }

  /**
   * Return this configuration as a rendered XML string.
   *
   * @return configuration XML string
   */
  public String asRenderedDocument() {
    return renderedDocument;
  }

  @Override
  public String toString() {
    return asRenderedDocument();
  }

  private static Element unparseEhcacheConfiguration(Configuration config) {
    if (config instanceof XmlConfiguration) {
      return ((XmlConfiguration) config).asDocument().getDocumentElement();
    } else {
      return new XmlConfiguration(config).asDocument().getDocumentElement();
    }
  }

  private interface Config {

    Configuration configuration() throws IllegalStateException;

    Configuration configuration(String variant);

    Configurations.Configuration unparse(ObjectFactory factory, Configurations.Configuration container);

    Set<String> variants();
  }

  private static class SingleConfig implements Config {

    private final Configuration config;

    private SingleConfig(Configuration config) {
      this.config = config;
    }

    @Override
    public Configuration configuration() {
      return config;
    }

    @Override
    public Configuration configuration(String variant) {
      return configuration();
    }

    @Override
    public Configurations.Configuration unparse(ObjectFactory factory, Configurations.Configuration container) {
      return container.withConfig(unparseEhcacheConfiguration(config));
    }

    @Override
    public Set<String> variants() {
      return emptySet();
    }
  }

  private static class VariantConfig implements Config {

    private final Map<String, Configuration> configs;

    private VariantConfig(Map<String, Configuration> configs) {
      this.configs = configs;
    }

    @Override
    public Configuration configuration() {
      switch (configs.size()) {
        case 0:
          return null;
        case 1:
          return configs.values().iterator().next();
        default:
          throw new IllegalStateException("Please choose a variant: " + configs.keySet());
      }
    }

    @Override
    public Configuration configuration(String variant) {
      Configuration configuration = configs.get(variant);
      if (configuration == null) {
        throw new IllegalArgumentException("Please choose a valid variant: " + configs.keySet());
      } else {
        return configuration;
      }
    }

    @Override
    public Configurations.Configuration unparse(ObjectFactory factory, Configurations.Configuration container) {
      return container.withVariant(configs.entrySet().stream()
        .map(v -> factory.createConfigurationsConfigurationVariant()
          .withType(v.getKey())
          .withConfig(unparseEhcacheConfiguration(v.getValue())))
        .collect(toList()));
    }

    @Override
    public Set<String> variants() {
      return unmodifiableSet(configs.keySet());
    }
  }

  /**
   * Create a builder seeded from an XML configuration.
   * <p>
   * Enclosed configurations will parsed using {@link XmlConfiguration#XmlConfiguration(Document)}.
   *
   * @param xml xml seed resource
   * @return a builder seeded with the xml configuration
   * @see XmlConfiguration#XmlConfiguration(Document)
   */
  public static Builder from(URL xml) {
    return from(new XmlMultiConfiguration(xml, (identity, dom) -> new XmlConfiguration(dom)));
  }

  /**
   * Create a builder seeded from an XML configuration using the supplier class loader.
   * <p>
   * Enclosed configurations will parsed using {@link XmlConfiguration#XmlConfiguration(Document, ClassLoader)}, which
   * will be passed the classloader provided to this method.
   *
   * @param xml xml seed resource
   * @param classLoader loader for the cache managers
   * @return a builder seeded with the xml configuration
   * @see XmlConfiguration#XmlConfiguration(Document, ClassLoader)
   */
  public static Builder from(URL xml, ClassLoader classLoader) {
    return from(new XmlMultiConfiguration(xml, (identity, dom) -> new XmlConfiguration(dom, classLoader)));
  }

  /**
   * Create a builder seeded from an existing {@code XmlMultiConfiguration}.
   *
   * @param config existing configuration seed
   * @return a builder seeded with the xml configuration
   */
  public static Builder from(XmlMultiConfiguration config) {
    return new Builder() {
      @Override
      public Builder withManager(String identity, Configuration configuration) {
        Map<String, Config> configurations = new HashMap<>(config.configurations);
        configurations.put(identity, new SingleConfig(configuration));
        return from(new XmlMultiConfiguration(configurations));
      }

      @Override
      public Builder withoutManager(String identity) {
        Map<String, Config> configurations = config.configurations;
        configurations.remove(identity);
        return from(new XmlMultiConfiguration(configurations));
      }

      @Override
      public Variant withManager(String identity) {
        Map<String, Configuration> variants = new HashMap<>();

        Config current = config.configurations.get(identity);
        if (current instanceof VariantConfig) {
          variants.putAll(((VariantConfig) current).configs);
        } else if (current != null) {
          throw new IllegalStateException("Existing non-variant configuration cannot be replaced - it must be removed first.");
        }

        return new Variant() {
          @Override
          public Variant withoutVariant(String variant) {
            variants.remove(variant);
            return this;
          }

          @Override
          public Variant variant(String variant, Configuration configuration) {
            variants.put(variant, configuration);
            return this;
          }

          @Override
          public Builder withoutManager(String identity) {
            return from(build()).withoutManager(identity);
          }

          @Override
          public Builder withManager(String identity, Configuration configuration) {
            return from(build()).withManager(identity, configuration);
          }

          @Override
          public Variant withManager(String identity) {
            return from(build()).withManager(identity);
          }

          @Override
          public XmlMultiConfiguration build() {
            Map<String, Config> configurations = new HashMap<>(config.configurations);
            configurations.put(identity, new VariantConfig(variants));
            return new XmlMultiConfiguration(configurations);
          }
        };
      }

      @Override
      public XmlMultiConfiguration build() {
        return config;
      }
    };
  }

  /**
   * Create an initially empty builder.
   *
   * @return an empty builder
   */
  public static Builder fromNothing() {
    return from(new XmlMultiConfiguration(emptyMap()));
  }

  /**
   * An {@code XmlMultiConfiguration} builder.
   */
  public interface Builder {

    /**
     * Remove the configuration with the given identity
     *
     * @param identity configuration to remove
     * @return a new builder instance
     */
    Builder withoutManager(String identity);

    /**
     * Add a new configuration with the given identity
     *
     * @param identity configuration identifier
     * @param configuration configuration instance
     * @return a new builder instance
     */
    Builder withManager(String identity, Configuration configuration);

    /**
     * Add a new configuration with the given identity built from the given builder.
     *
     * @param identity configuration identifier
     * @param builder configuration builder
     * @return a new builder instance
     */
    default Builder withManager(String identity, org.ehcache.config.Builder<? extends Configuration> builder) {
      return withManager(identity, builder.build());
    }

    /**
     * Add a new manager with variant configurations.
     *
     * @param identity configuration to add
     * @return a new variant configuration builder
     */
    Variant withManager(String identity);

    /**
     * Build a new {@code XmlMultiConfiguration}.
     *
     * @return a new {@code XmlMultiConfiguration}
     */
    XmlMultiConfiguration build();
  }

  /**
   * A variant configuration builder.
   */
  public interface Variant extends Builder {

    /**
     * Remove the given configuration variant.
     *
     * @param variant variant to remove
     * @return a new builder instance
     */
    Variant withoutVariant(String variant);

    /**
     * Add a new variant configuration
     *
     * @param variant configuration variant
     * @param configuration configuration instance
     * @return a new builder instance
     */
    Variant variant(String variant, Configuration configuration);

    /**
     * Add a new variant configuration built from the given builder.
     *
     * @param variant configuration variant
     * @param builder configuration builder
     * @return a new builder instance
     */
    default Variant variant(String variant, org.ehcache.config.Builder<? extends Configuration> builder) {
      return variant(variant, builder.build());
    }
  }
}
