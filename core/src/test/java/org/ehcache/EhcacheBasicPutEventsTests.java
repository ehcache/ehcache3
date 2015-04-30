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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
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
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsCreated;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;


/**
 * The class tries to test some of the eventing mechanism for basic PUT operations.
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class EhcacheBasicPutEventsTests extends EhcacheBasicCrudBase{

    @Mock
    protected CacheLoaderWriter<String, String> cacheLoaderWriter;

    @Mock
    protected CacheEventListener<String,String> cacheEventListener;

    protected CacheEventNotificationService<String,String> cacheEventNotificationService;

    protected IsCreated isCreated = new IsCreated();

    @Test
    public void testPutNullNull() {
        final Ehcache<String, String> ehcache = this.getEhcache(null);

        try {
            ehcache.put(null, null);
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
        ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
    }

    @Test
    public void testPutKeyNull() {
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        try {
            ehcache.put("key", null);
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
        ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
    }

    @Test
    public void testPutNullValue() {
        final Ehcache<String, String> ehcache = this.getEhcache(null);

        try {
            ehcache.put(null, "value");
            fail();
        } catch (NullPointerException e) {
            // expected
        }
        verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
        ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
    }

    @Test
    public void testPut() throws Exception {
        final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
        this.store = spy(fakeStore);
        final Ehcache<String, String> ehcache = this.getEhcache(null);
        ehcache.put("key", "value");
        verify(cacheEventListener,times(1)).onEvent(argThat(isCreated));
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    @Test
    public void testPutNoStoreEntryCacheAccessExceptionCacheLoaderWriterException() throws Exception {
        final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
        this.store = spy(fakeStore);
        doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());
        doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
        final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
        try {
            ehcache.put("key", "value");
            fail();
        } catch (CacheWritingException e) {
            // Expected
        }
        verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }


    @Test
    public void testPutExceptionCacheLoaderWriterException() throws Exception {
        final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
        this.store = spy(fakeStore);
        doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
        final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
        try {
            ehcache.put("key", "value");
            fail();
        } catch (CacheWritingException e) {
            // Expected
        }
        verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
            fail();
        } catch (UnsupportedOperationException e){
            //expected
        }
    }

    @Test
    public void testPutHasStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
        final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
        this.store = spy(fakeStore);
        doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

        final Ehcache<String, String> ehcache = this.getEhcache(null);

        ehcache.put("key", "value");
        verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
        try {
            ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
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
                LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicPutEventsTest"));
        ehcache.init();
        assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
        registerCacheEventListener(ehcache, cacheEventListener);
        return ehcache;
    }

}