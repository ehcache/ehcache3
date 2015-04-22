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

package org.ehcache;

import static org.ehcache.EhcacheBasicBulkUtil.KEY_SET_A;
import static org.ehcache.EhcacheBasicBulkUtil.KEY_SET_B;
import static org.ehcache.EhcacheBasicBulkUtil.KEY_SET_C;
import static org.ehcache.EhcacheBasicBulkUtil.KEY_SET_D;
import static org.ehcache.EhcacheBasicBulkUtil.fanIn;
import static org.ehcache.EhcacheBasicBulkUtil.getAltEntryMap;
import static org.ehcache.EhcacheBasicBulkUtil.getEntryMap;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEventNotificationServiceImpl;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsCreatedOrUpdated;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

/**
 * Provides events based testing for PUT_ALL operations
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class EhcacheBasicPutAllEventsTest extends EhcacheBasicCrudBase{

    @Mock
    protected CacheLoaderWriter<String, String> cacheLoaderWriter;

    @Mock
    protected CacheEventListener testCacheEventListener;

    protected CacheEventNotificationService cacheEventNotificationService;

    protected IsCreatedOrUpdated isCreatedOrUpdated = new IsCreatedOrUpdated();

    @Test
    @SuppressWarnings("unchecked")
    public void testPutAllNull() throws Exception {
        final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
        final FakeStore fakeStore = new FakeStore(originalStoreContent);
        this.store = spy(fakeStore);
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        try {
            ehcache.putAll(null);
            fail();
        } catch (NullPointerException e) {
            // Expected
        }
        verify(testCacheEventListener,never()).onEvent(any(CacheEvent.class));
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPutAllNullKey() throws Exception {
        final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
        final FakeStore fakeStore = new FakeStore(originalStoreContent);
        this.store = spy(fakeStore);

        final Map<String, String> entries = new LinkedHashMap<String, String>();
        for (final Map.Entry<String, String> entry : getEntryMap(KEY_SET_A).entrySet()) {
            final String key = entry.getKey();
            entries.put(key, entry.getValue());
            if ("keyA2".equals(key)) {
                entries.put(null, "nullKey");
            }
        }
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        try {
            ehcache.putAll(entries);
            fail();
        } catch (NullPointerException e) {
            // Expected
        }
        verify(testCacheEventListener,never()).onEvent(any(CacheEvent.class));
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPutAllNullValue() throws Exception {
        final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
        final FakeStore fakeStore = new FakeStore(originalStoreContent);
        this.store = spy(fakeStore);

        final Map<String, String> entries = new LinkedHashMap<String, String>();
        for (final Map.Entry<String, String> entry : getEntryMap(KEY_SET_A).entrySet()) {
            final String key = entry.getKey();
            entries.put(key, entry.getValue());
            if ("keyA2".equals(key)) {
                entries.put("keyA2a", null);
            }
        }
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        try {
            ehcache.putAll(entries);
            fail();
        } catch (NullPointerException e) {
            // Expected
        }
        verify(testCacheEventListener,never()).onEvent(any(CacheEvent.class));
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    @Test
    public void testPutAll() throws Exception {
        final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
        final FakeStore fakeStore = new FakeStore(originalStoreContent);
        this.store = spy(fakeStore);
        final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
        final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
        ehcache.putAll(contentUpdates);
        verify(testCacheEventListener,times(18)).onEvent(argThat(isCreatedOrUpdated));
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPutAllEmptyRequestCacheAccessExceptionBeforeNoWriter() throws Exception {
        final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
        final FakeStore fakeStore = new FakeStore(originalStoreContent);
        this.store = spy(fakeStore);
        doThrow(new CacheAccessException("")).when(this.store)
                .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        ehcache.putAll(Collections.<String, String>emptyMap());
        verify(testCacheEventListener,never()).onEvent(any(CacheEvent.class));
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    /**
     * Returns a Mockito {@code any} Matcher for {@code java.util.Set<String>}.
     *
     * @return a Mockito {@code any} matcher for {@code Set<String>}
     */
    @SuppressWarnings("unchecked")
    private static Set<? extends String> getAnyStringSet() {
        return any(Set.class);   // unchecked
    }

    /**
     * Returns a Mockito {@code any} Matcher for a {@link org.ehcache.function.Function} over a {@code Map.Entry} {@code Iterable}.
     *
     * @return a Mockito {@code any} matcher for {@code Function}
     */
    @SuppressWarnings("unchecked")
    private static Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> getAnyEntryIterableFunction() {
        return any(Function.class);   // unchecked
    }

    private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
        ExecutorService orderedExecutor = Executors.newSingleThreadExecutor();
        ExecutorService unorderedExecutor = Executors.newCachedThreadPool();
        cacheEventNotificationService = new CacheEventNotificationServiceImpl<String, String>(orderedExecutor, unorderedExecutor);
        final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store,
                cacheLoaderWriter,cacheEventNotificationService,null,
                LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicPutAllEventsTest"));
        ehcache.init();
        assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
        registerCacheEventListener(ehcache, testCacheEventListener);
        return ehcache;
    }

}