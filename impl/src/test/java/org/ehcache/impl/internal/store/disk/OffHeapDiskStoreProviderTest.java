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

package org.ehcache.impl.internal.store.disk;

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.DefaultTimeSourceService;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;

import java.util.Set;

import static java.util.Collections.singleton;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 * OffHeapStoreProviderTest
 */
public class OffHeapDiskStoreProviderTest {

  @Test
   public void testStatisticsAssociations() throws Exception {
     OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();

    ServiceLocator serviceLocator = dependencySet().with(mock(SerializationProvider.class))
      .with(new DefaultTimeSourceService(null)).with(mock(DiskResourceService.class)).build();
    provider.start(serviceLocator);

    OffHeapDiskStore<Long, String> store = provider.createStore(getStoreConfig(), mock(PersistableResourceService.PersistenceSpaceIdentifier.class));

    @SuppressWarnings("unchecked")
    Query storeQuery = queryBuilder()
      .children()
      .filter(context(attributes(Matchers.allOf(
        hasAttribute("tags", new Matcher<Set<String>>() {
          @Override
          protected boolean matchesSafely(Set<String> object) {
            return object.contains("Disk");
          }
        })))))
      .build();

     Set<TreeNode> nodes = singleton(ContextManager.nodeFor(store));

     Set<TreeNode> storeResult = storeQuery.execute(nodes);
     assertThat(storeResult, not(empty()));

     provider.releaseStore(store);

     storeResult = storeQuery.execute(nodes);
     assertThat(storeResult, empty());
   }

   private Store.Configuration<Long, String> getStoreConfig() {
     return new Store.Configuration<Long, String>() {
       @Override
       public Class<Long> getKeyType() {
         return Long.class;
       }

       @Override
       public Class<String> getValueType() {
         return String.class;
       }

       @Override
       public EvictionAdvisor<? super Long, ? super String> getEvictionAdvisor() {
         return Eviction.noAdvice();
       }

       @Override
       public ClassLoader getClassLoader() {
         return getClass().getClassLoader();
       }

       @Override
       public ExpiryPolicy<? super Long, ? super String> getExpiry() {
         return ExpiryPolicyBuilder.noExpiration();
       }

       @Override
       public ResourcePools getResourcePools() {
         return new ResourcePools() {
           @Override
           @SuppressWarnings("unchecked")
           public <P extends ResourcePool> P getPoolForResource(ResourceType<P> resourceType) {
             return (P) new SizedResourcePool() {
               @Override
               public ResourceType<SizedResourcePool> getType() {
                 return ResourceType.Core.DISK;
               }

               @Override
               public long getSize() {
                 return 1;
               }

               @Override
               public ResourceUnit getUnit() {
                 return MemoryUnit.MB;
               }

               @Override
               public boolean isPersistent() {
                 return false;
               }

               @Override
               public void validateUpdate(ResourcePool newPool) {
                 throw new UnsupportedOperationException("TODO Implement me!");
               }
             };
           }

           @Override
           @SuppressWarnings("unchecked")
           public Set<ResourceType<?>> getResourceTypeSet() {
             return (Set) singleton(ResourceType.Core.OFFHEAP);
           }

           @Override
           public ResourcePools validateAndMerge(ResourcePools toBeUpdated) throws IllegalArgumentException, UnsupportedOperationException {
             throw new UnsupportedOperationException("TODO Implement me!");
           }
         };
       }

       @Override
       public Serializer<Long> getKeySerializer() {
         return new LongSerializer();
       }

       @Override
       public Serializer<String> getValueSerializer() {
         return new StringSerializer();
       }

       @Override
       public int getDispatcherConcurrency() {
         return 1;
       }

       @Override
       public CacheLoaderWriter<? super Long, String> getCacheLoaderWriter() {
         return null;
       }

     };
   }}
