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
import static org.ehcache.EhcacheBasicBulkUtil.copyWithout;
import static org.ehcache.EhcacheBasicBulkUtil.fanIn;
import static org.ehcache.EhcacheBasicBulkUtil.getEntryMap;
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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEventNotificationServiceImpl;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsRemoved;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

/**
 * This class provides the events based for REMOVE_ALL operations.
 */
public class EhcacheBasicRemoveAllEventsTest extends EhcacheBasicCrudBase {

    @Mock
    protected CacheEventListener testCacheEventListener;

    protected CacheEventNotificationService<String,String> cacheEventNotificationService;

    protected IsRemoved isRemoved = new IsRemoved();

    @Mock
    protected CacheLoaderWriter<String, String> cacheLoaderWriter;

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveAllNull() throws Exception {
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        try {
            ehcache.removeAll(null);
            fail();
        } catch (NullPointerException e) {
            // Expected
        }
        verify(testCacheEventListener,never()).onEvent(any(CacheEvent.class));
        ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveAllNullKey() throws Exception {
        final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
        final FakeStore fakeStore = new FakeStore(originalStoreContent);
        this.store = spy(fakeStore);
        final Set<String> keys = new LinkedHashSet<String>();
        for (final String key : KEY_SET_A) {
            keys.add(key);
            if ("keyA2".equals(key)) {
                keys.add(null);
            }
        }
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        try {
            ehcache.removeAll(keys);
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
    public void testRemoveAll() throws Exception {
        final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
        final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
        this.store = spy(fakeStore);
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
        ehcache.removeAll(contentUpdates);
        verify(testCacheEventListener,times(5)).onEvent(argThat(isRemoved));
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveAllComplete() throws Exception {
        final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
        final FakeStore fakeStore = new FakeStore(originalStoreContent);
        this.store = spy(fakeStore);
        final Ehcache<String, String> ehcache = this.getEhcache(null);

        final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
        ehcache.removeAll(contentUpdates);
        verify(testCacheEventListener,times(12)).onEvent(argThat(isRemoved));
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterNoOverlapSomeFail() throws Exception {
        final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
        final FakeStore fakeStore = new FakeStore(originalStoreContent);
        this.store = spy(fakeStore);
        doThrow(new CacheAccessException("")).when(this.store)
                .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

        final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
        final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
        this.cacheLoaderWriter = spy(fakeLoaderWriter);

        final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

        final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
        final Set<String> expectedFailures = KEY_SET_C;
        final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
        try {
            ehcache.removeAll(contentUpdates);
            fail();
        } catch (BulkCacheWritingException e) {
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
                LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicRemoveAllEventsTest"));
        ehcache.init();
        assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
        super.registerCacheEventListener(ehcache, testCacheEventListener);
        return ehcache;
    }

}
