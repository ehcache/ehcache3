package org.ehcache.clustered.laurentmerckx;

import org.ehcache.clustered.ClusteredTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class LaurentMerckxTest extends ClusteredTests {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"heap-resource\" unit=\"MB\">20</ohr:resource>"
      + "</ohr:offheap-resources>"
      + "</config>\n";

  @ClassRule
  public static Cluster CLUSTER = newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Test
  public void testLaurent() throws IOException {
    {
      CacheComponent component = new CacheComponent(CLUSTER.getConnectionURI());

      ProviderArguments key = new ProviderArguments();
      key.setEnterpriseNo("xxxxx");
      key.setLanguageCode(LanguageCode.ENGLISH);
      key.setModuleName("TEST");
      key.setModuleParameters(Collections.singletonMap("param1", "value1"));
      key.setGlobalParameters(Collections.singletonMap("param2", "value2"));

      assertThat(component.get(key), nullValue());
      component.put(key, new CacheContent(ContentType.JSON, Encoding.UTF8, "{\"test\": \"value\"}".getBytes()));
    }

    {
      CacheComponent component = new CacheComponent(CLUSTER.getConnectionURI());

      ProviderArguments key = new ProviderArguments();
      key.setEnterpriseNo("xxxxx");
      key.setLanguageCode(LanguageCode.ENGLISH);
      key.setModuleName("TEST");
      key.setModuleParameters(Collections.singletonMap("param1", "value1"));
      key.setGlobalParameters(Collections.singletonMap("param2", "value2"));

      assertThat(component.get(key), notNullValue());
    }
  }
}
