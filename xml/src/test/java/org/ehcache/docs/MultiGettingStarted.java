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

package org.ehcache.docs;

import org.ehcache.config.Configuration;
import org.ehcache.config.ResourceType;
import org.ehcache.xml.multi.XmlMultiConfiguration;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsMapContaining;
import org.hamcrest.core.AllOf;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MultiGettingStarted {

  @Test
  public void multipleConfigurations() {
    //tag::multipleManagers[]
    XmlMultiConfiguration multipleConfiguration = XmlMultiConfiguration
      .from(getClass().getResource("/configs/docs/multi/multiple-managers.xml")) // <1>
      .build(); // <2>

    Configuration fooConfiguration = multipleConfiguration.configuration("foo-manager"); // <3>
    //end::multipleManagers[]

    Assert.assertThat(resourceMap(multipleConfiguration.identities().stream().collect(
      Collectors.toMap(Function.identity(), multipleConfiguration::configuration)
    )), AllOf.allOf(
      IsMapContaining.hasEntry(Is.is("foo-manager"), IsMapContaining.hasEntry(Is.is("foo"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP, ResourceType.Core.OFFHEAP))),
      IsMapContaining.hasEntry(Is.is("bar-manager"), IsMapContaining.hasEntry(Is.is("bar"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP, ResourceType.Core.OFFHEAP)))
    ));
  }

  @Test
  public void multipleVariants() {
    //tag::multipleVariants[]
    XmlMultiConfiguration variantConfiguration = XmlMultiConfiguration
      .from(getClass().getResource("/configs/docs/multi/multiple-variants.xml"))
      .build();

    Configuration fooConfiguration = variantConfiguration.configuration("foo-manager", "offheap"); // <1>
    //end::multipleVariants[]

    Assert.assertThat(resourceMap(variantConfiguration.identities().stream().collect(
      Collectors.toMap(Function.identity(), i -> variantConfiguration.configuration(i, "offheap"))
    )), AllOf.allOf(
      IsMapContaining.hasEntry(Is.is("foo-manager"), IsMapContaining.hasEntry(Is.is("foo"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP, ResourceType.Core.OFFHEAP))),
      IsMapContaining.hasEntry(Is.is("bar-manager"), IsMapContaining.hasEntry(Is.is("bar"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP)))
    ));

    Assert.assertThat(resourceMap(variantConfiguration.identities().stream().collect(
      Collectors.toMap(Function.identity(), i -> variantConfiguration.configuration(i, "heap"))
    )), AllOf.allOf(
      IsMapContaining.hasEntry(Is.is("foo-manager"), IsMapContaining.hasEntry(Is.is("foo"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP))),
      IsMapContaining.hasEntry(Is.is("bar-manager"), IsMapContaining.hasEntry(Is.is("bar"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP)))
    ));
  }

  @Test
  public void multipleRetrieval() {
    XmlMultiConfiguration multipleConfiguration = XmlMultiConfiguration
      .from(getClass().getResource("/configs/docs/multi/multiple-managers.xml"))
      .build();
    XmlMultiConfiguration variantConfiguration = XmlMultiConfiguration
      .from(getClass().getResource("/configs/docs/multi/multiple-variants.xml"))
      .build();

    //tag::multipleRetrieval[]
    Map<String, Configuration> allConfigurations = multipleConfiguration.identities().stream() // <1>
      .collect(Collectors.toMap(i -> i, i -> multipleConfiguration.configuration(i))); // <2>
    Map<String, Configuration> offheapConfigurations = variantConfiguration.identities().stream()
      .collect(Collectors.toMap(i -> i, i -> variantConfiguration.configuration(i, "offheap"))); // <3>
    //end::multipleRetrieval[]

    Assert.assertThat(resourceMap(allConfigurations), AllOf.allOf(
      IsMapContaining.hasEntry(Is.is("foo-manager"), IsMapContaining.hasEntry(Is.is("foo"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP, ResourceType.Core.OFFHEAP))),
      IsMapContaining.hasEntry(Is.is("bar-manager"), IsMapContaining.hasEntry(Is.is("bar"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP, ResourceType.Core.OFFHEAP)))
    ));

    Assert.assertThat(resourceMap(offheapConfigurations), AllOf.allOf(
      IsMapContaining.hasEntry(Is.is("foo-manager"), IsMapContaining.hasEntry(Is.is("foo"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP, ResourceType.Core.OFFHEAP))),
      IsMapContaining.hasEntry(Is.is("bar-manager"), IsMapContaining.hasEntry(Is.is("bar"), IsIterableContainingInAnyOrder.containsInAnyOrder(ResourceType.Core.HEAP)))
    ));
  }

  @Test
  public void building() {
    XmlMultiConfiguration sourceConfiguration = XmlMultiConfiguration
      .from(getClass().getResource("/configs/docs/multi/multiple-variants.xml"))
      .build();
    Configuration barConfiguration = sourceConfiguration.configuration("bar-manager");
    Configuration heapConfiguration = sourceConfiguration.configuration("foo-manager", "heap");
    Configuration offheapConfiguration = sourceConfiguration.configuration("foo-manager", "offheap");

    //tag::building[]
    XmlMultiConfiguration multiConfiguration = XmlMultiConfiguration.fromNothing() // <1>
      .withManager("bar", barConfiguration) // <2>
      .withManager("foo").variant("heap", heapConfiguration).variant("offheap", offheapConfiguration) // <3>
      .build(); // <4>
    //end::building[]

    //tag::modifying[]
    XmlMultiConfiguration modified = XmlMultiConfiguration.from(multiConfiguration) // <1>
      .withManager("foo") // <2>
      .build();
    //end::modifying[]

    //tag::rendering[]
    String xmlString = multiConfiguration.asRenderedDocument(); // <1>
    Document xmlDocument = multiConfiguration.asDocument(); // <2>
    //end::rendering[]
  }

  private static Map<String, Map<String, Set<ResourceType<?>>>> resourceMap(Map<String, Configuration> configurations) {
    return configurations.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
      manager -> manager.getValue().getCacheConfigurations().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
        cache -> cache.getValue().getResourcePools().getResourceTypeSet()))));
  }
}
