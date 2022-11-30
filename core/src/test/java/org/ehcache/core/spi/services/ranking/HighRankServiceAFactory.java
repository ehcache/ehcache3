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
package org.ehcache.core.spi.services.ranking;

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.spi.service.ServiceCreationConfiguration;

public class HighRankServiceAFactory implements ServiceFactory<RankServiceA> {

  @Override
  public int rank() {
    return 2;
  }

  @Override
  public RankServiceA create(ServiceCreationConfiguration<RankServiceA, ?> configuration) {
    return new RankServiceA("high-rank");
  }

  @Override
  public Class<? extends RankServiceA> getServiceType() {
    return RankServiceA.class;
  }
}
