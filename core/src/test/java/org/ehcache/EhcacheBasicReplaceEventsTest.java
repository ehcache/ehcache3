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

import static org.hamcrest.CoreMatchers.equalTo;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEventNotificationServiceImpl;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsUpdated;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

/**
 * This class provides testing of events for basic REPLACE operations.
 *
 */
public class EhcacheBasicReplaceEventsTest extends EhcacheBasicCrudBase{

    @Mock
    protected CacheLoaderWriter<String, String> cacheLoaderWriter;

    @Mock
    protected CacheEventListener<String,String> testCacheEventListener;

    protected CacheEventNotificationService cacheEventNotificationService;

    protected IsUpdated isUpdated = new IsUpdated();

    @Test
    @SuppressWarnings("unchecked")
    public void testReplaceNullNull() {
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        
        try {
            ehcache.replace(null, null);
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        verify(testCacheEventListener,never()).onEvent(any(CacheEvent.class));
        ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReplaceKeyNull() {
        final Ehcache<String, String> ehcache = this.getEhcache(null);

        try {
            ehcache.replace("key", null);
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        verify(testCacheEventListener,never()).onEvent(any(CacheEvent.class));
        ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReplaceNullValue() {
        final Ehcache<String, String> ehcache = this.getEhcache(null);

        try {
            ehcache.replace(null, "value");
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        verify(testCacheEventListener,never()).onEvent(any(CacheEvent.class));
        ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReplace() throws Exception {
        final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
        this.store = spy(fakeStore);
        final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
        final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);
        assertThat(ehcache.replace("key", "value"), is(equalTo("oldValue")));
        verify(testCacheEventListener,times(1)).onEvent(argThat(isUpdated));
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(testCacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReplaceHasStoreEntryCacheWritingException() throws Exception {
        final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
        this.store = spy(fakeStore);
        doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
        final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
        try {
            ehcache.replace("key", "value");
            fail();
        } catch (CacheWritingException e) {
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

    private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
        ExecutorService orderedExecutor = Executors.newSingleThreadExecutor();
        ExecutorService unorderedExecutor = Executors.newCachedThreadPool();
        cacheEventNotificationService = new CacheEventNotificationServiceImpl<String, String>(orderedExecutor, unorderedExecutor);
        final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store,
                cacheLoaderWriter,cacheEventNotificationService,null,
                LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicReplaceEventsTest"));
        ehcache.init();
        assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
        super.registerCacheEventListener(ehcache, testCacheEventListener);
        return ehcache;
    }

}
