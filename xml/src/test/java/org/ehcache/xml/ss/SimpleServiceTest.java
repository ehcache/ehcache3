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
package org.ehcache.xml.ss;

import com.pany.ehcache.service.AccountingService;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class SimpleServiceTest {

  @Test
  public void name() {
    XmlConfiguration xmlConfiguration = new XmlConfiguration(getClass().getResource("/configs/simple-service.xml"));

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManager(xmlConfiguration, true)) {
    }

    assertThat(AccountingService.ACTIONS.get(0)).isSameAs(AccountingService.Action.START);
    assertThat(AccountingService.ACTIONS.get(1)).isSameAs(AccountingService.Action.STOP);
    assertThat(AccountingService.ACTIONS.size()).isEqualTo(2);
  }

}
