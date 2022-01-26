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

package org.ehcache.xml.service;

import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder.BatchedWriteBehindConfigurationBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder.UnBatchedWriteBehindConfigurationBuilder;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.xml.model.BaseCacheType;
import org.ehcache.xml.model.CacheLoaderWriterType;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.TimeType;

import java.math.BigInteger;

import static java.util.Optional.ofNullable;
import static org.ehcache.config.builders.WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration;
import static org.ehcache.xml.XmlModel.convertToJUCTimeUnit;
import static org.ehcache.xml.XmlModel.convertToXmlTimeUnit;

public class DefaultWriteBehindConfigurationParser
  extends SimpleCoreServiceConfigurationParser<CacheLoaderWriterType.WriteBehind, CacheLoaderWriterType, WriteBehindConfiguration<?>> {

  @SuppressWarnings("unchecked")
  public DefaultWriteBehindConfigurationParser() {
    super((Class<WriteBehindConfiguration<?>>) (Class) WriteBehindConfiguration.class,
      CacheTemplate::writeBehind,
      config -> ofNullable(config.getBatching()).<WriteBehindConfigurationBuilder>map(batching -> {
        BatchedWriteBehindConfigurationBuilder batchedBuilder = newBatchedWriteBehindConfiguration(batching.getMaxWriteDelay().getValue().longValue(), convertToJUCTimeUnit(batching.getMaxWriteDelay().getUnit()), batching.getBatchSize().intValue());
        if (batching.isCoalesce()) {
          batchedBuilder = batchedBuilder.enableCoalescing();
        }
        return batchedBuilder;
      }).orElseGet(UnBatchedWriteBehindConfigurationBuilder::newUnBatchedWriteBehindConfiguration).useThreadPool(config.getThreadPool())
        .concurrencyLevel(config.getConcurrency().intValue())
        .queueSize(config.getSize().intValue()).build(),
      BaseCacheType::getLoaderWriter, BaseCacheType::setLoaderWriter,
      config -> {
        CacheLoaderWriterType.WriteBehind writeBehind = new CacheLoaderWriterType.WriteBehind()
          .withThreadPool(config.getThreadPoolAlias())
          .withConcurrency(BigInteger.valueOf(config.getConcurrency()))
          .withSize(BigInteger.valueOf(config.getMaxQueueSize()));

        WriteBehindConfiguration.BatchingConfiguration batchingConfiguration = config.getBatchingConfiguration();
        if (batchingConfiguration == null) {
          writeBehind.setNonBatching(new CacheLoaderWriterType.WriteBehind.NonBatching());
        } else {
          writeBehind.withBatching(new CacheLoaderWriterType.WriteBehind.Batching()
            .withBatchSize(BigInteger.valueOf(batchingConfiguration.getBatchSize()))
            .withCoalesce(batchingConfiguration.isCoalescing())
            .withMaxWriteDelay(new TimeType()
              .withValue(BigInteger.valueOf(batchingConfiguration.getMaxDelay()))
              .withUnit(convertToXmlTimeUnit(batchingConfiguration.getMaxDelayUnit()))
            )
          );
        }
        return new CacheLoaderWriterType().withWriteBehind(writeBehind);
      },
      (existing, additional) -> {
        existing.setWriteBehind(additional.getWriteBehind());
        return existing;
      });
  }
}
