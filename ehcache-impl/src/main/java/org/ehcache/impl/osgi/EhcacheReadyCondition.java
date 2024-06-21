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
package org.ehcache.impl.osgi;

import org.ehcache.core.EhcacheManager;
import org.ehcache.core.events.CacheEventDispatcherFactory;
import org.ehcache.core.events.CacheEventListenerProvider;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.spi.resilience.DefaultResilienceStrategyProvider;
import org.ehcache.impl.internal.store.tiering.TieredStore;
import org.ehcache.impl.internal.store.tiering.TieredStoreProviderFactory;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindProvider;
import org.ehcache.spi.resilience.ResilienceStrategyProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.osgi.service.component.ComponentConstants;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.condition.Condition;

/**
 * The condition is satisfied once all default {@link ServiceFactory} classes mentioned in {@link EhcacheManager} or {@link EhcacheCachingProvider} are registered.
 * Those are
 * <ol>
 * <li>{@link Store.Provider} provided through DS component {@link TieredStoreProviderFactory}</li>
 * <li>{@link CacheLoaderWriterProvider} provided through DS component {@link DefaultCacheLoaderWriterProviderFactory}</li>
 * <li>{@link WriteBehindProvider} provided through DS component {@link WriteBehindProviderFactory}</li>
 * <li>{@link CacheEventDispatcherFactory} provided through DS component {@link CacheEventNotificationListenerServiceProviderFactory}</li>
 * <li>{@link CacheEventListenerProvider} provided through DS component {@link DefaultCacheEventListenerProviderFactory}</li>
 * <li>{@link ResilienceStrategyProvider} provided through DS component {@link DefaultResilienceStrategyProvider}</li>
 * <li>{@link SerializationProvider} provided through DS component {@link DefaultSerializationProviderFactory}</li>
 * </ol>
 * Because those services are not provided directly but through {@link ServiceFactory} this condition references directly the responsible default factories (via their component name)
 */
@Component(property = Condition.CONDITION_ID + "=ehcache")
public class EhcacheReadyCondition implements Condition {

    @Reference(target = "("+ComponentConstants.COMPONENT_NAME+"=org.ehcache.impl.internal.store.tiering.TieredStoreProviderFactory)")
    private ServiceFactory<TieredStore.Provider> storeProviderFactory;

    @Reference(target = "("+ComponentConstants.COMPONENT_NAME+"=org.ehcache.impl.internal.spi.loaderwriter.DefaultCacheLoaderWriterProviderFactory)")
    private ServiceFactory<CacheLoaderWriterProvider> cacheLoaderWriterProviderFactory;

    @Reference(target = "("+ComponentConstants.COMPONENT_NAME+"=org.ehcache.impl.internal.loaderwriter.writebehind.WriteBehindProviderFactory)")
    private ServiceFactory<WriteBehindProvider> writeBehindProviderFactory;

    @Reference(target = "("+ComponentConstants.COMPONENT_NAME+"=org.ehcache.impl.internal.events.CacheEventNotificationListenerServiceProviderFactory)")
    private ServiceFactory<CacheEventDispatcherFactory> cacheEventNotificationListenerServiceProviderFactory;

    @Reference(target = "("+ComponentConstants.COMPONENT_NAME+"=org.ehcache.impl.internal.spi.event.DefaultCacheEventListenerProviderFactory)")
    private ServiceFactory<CacheEventListenerProvider> cacheEventListenerProviderFactory;

    @Reference(target = "("+ComponentConstants.COMPONENT_NAME+"=org.ehcache.impl.internal.spi.resilience.DefaultResilienceStrategyProviderFactory)")
    private ServiceFactory<ResilienceStrategyProvider> resilienceStrategyProviderFactory;

    @Reference(target = "("+ComponentConstants.COMPONENT_NAME+"=org.ehcache.impl.internal.spi.serialization.DefaultSerializationProviderFactory)")
    private ServiceFactory<SerializationProvider> serializationProviderFactory;
}
