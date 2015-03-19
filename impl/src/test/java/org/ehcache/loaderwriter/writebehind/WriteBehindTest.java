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
package org.ehcache.loaderwriter.writebehind;

import static org.mockito.Matchers.anyObject;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.expiry.Expirations;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Abhilash
 *
 */
public class WriteBehindTest {
  
  private CacheManager cacheManager ;
  private Cache<String, String> testCache;
  private final WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();

  @Before
  public void setUp(){
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheLoaderWriterFactory cacheLoaderWriterFactory = mock(CacheLoaderWriterFactory.class);
    
    when(cacheLoaderWriterFactory.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);
    
    CacheLoaderWriterConfiguration cacheLoaderWriterConfiguration  = new CacheLoaderWriterConfiguration() {
      
      @Override
      public Class<CacheLoaderWriterFactory> getServiceType() {
        return CacheLoaderWriterFactory.class;
      }
      
      @Override
      public boolean writeCoalescing() {
        return false;
      }
      
      @Override
      public int writeBehindMaxQueueSize() {
        return 10;
      }
      
      @Override
      public int writeBehindConcurrency() {
        return 1;
      }
      
      @Override
      public boolean writeBatching() {
        return true;
      }
      
      @Override
      public int writeBatchSize() {
        return 5;
      }
      
      @Override
      public int retryAttempts() {
        return 0;
      }
      
      @Override
      public int retryAttemptDelaySeconds() {
        return 1;
      }
      
      @Override
      public int rateLimitPerSecond() {
        return 0;
      }
      
      @Override
      public int minWriteDelay() {
        return 0;
      }
      
      @Override
      public int maxWriteDelay() {
        return 0;
      }
      
      @Override
      public boolean isWriteBehind() {
        return true;
      }
    };
    
    builder.using(cacheLoaderWriterFactory);
    
    cacheManager = builder.build();
    testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().withExpiry(Expirations.noExpiration()).addServiceConfig(cacheLoaderWriterConfiguration).buildConfig(String.class, String.class));

  }
  
  @Test
  public void testWrites(){
    testCache.put("test1", "test1");
    testCache.put("test2", "test2");
    testCache.put("test3", "test3");
    testCache.remove("test2");
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertThat(loaderWriter.getData().get("test1").getValue(), is("test1"));
    assertThat(loaderWriter.getData().get("test2"), nullValue());
  }
  
  @Test
  public void testBatching(){
    for(int i=0 ; i<10; i++)
      testCache.put("test"+i, "test"+i);
    
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    
    assertThat(loaderWriter.getData().size(), is(10));
  }
  
//  @Ignore
//  @Test
//  public void testReads() throws BulkCacheWritingException, Exception{
//
//    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
//    CacheLoaderWriterFactory cacheLoaderWriterFactory = mock(CacheLoaderWriterFactory.class);
//    CacheLoaderWriter<String, String> cacheLoaderWriter = mock(WriteBehindTestLoaderWriter.class);
//    when(cacheLoaderWriterFactory.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)cacheLoaderWriter);
//    
//    CacheLoaderWriterConfiguration cacheLoaderWriterConfiguration  = new CacheLoaderWriterConfiguration() {
//      
//      @Override
//      public Class<CacheLoaderWriterFactory> getServiceType() {
//        return CacheLoaderWriterFactory.class;
//      }
//      
//      @Override
//      public boolean writeCoalescing() {
//        return false;
//      }
//      
//      @Override
//      public int writeBehindMaxQueueSize() {
//        return 10;
//      }
//      
//      @Override
//      public int writeBehindConcurrency() {
//        return 1;
//      }
//      
//      @Override
//      public boolean writeBatching() {
//        return true;
//      }
//      
//      @Override
//      public int writeBatchSize() {
//        return 5;
//      }
//      
//      @Override
//      public int retryAttempts() {
//        return 0;
//      }
//      
//      @Override
//      public int retryAttemptDelaySeconds() {
//        return 1;
//      }
//      
//      @Override
//      public int rateLimitPerSecond() {
//        return 0;
//      }
//      
//      @Override
//      public int minWriteDelay() {
//        return 0;
//      }
//      
//      @Override
//      public int maxWriteDelay() {
//        return 0;
//      }
//      
//      @Override
//      public boolean isWriteBehind() {
//        return true;
//      }
//    };
//    
//    builder.using(cacheLoaderWriterFactory);
//    
//    cacheManager = builder.build();
//    testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().withExpiry(Expirations.noExpiration()).addServiceConfig(cacheLoaderWriterConfiguration).buildConfig(String.class, String.class));
//
//    List<Map.Entry<String, String>> firstBatch = new ArrayList<Map.Entry<String,String>>();
//    List<Map.Entry<String, String>> secondBatch = new ArrayList<Map.Entry<String,String>>();
//    for(int i=0 ; i<10; i++){
//      final String val = "test"+i; 
//      testCache.put(val, val );
//      if(i <= 4) firstBatch.add(new Map.Entry<String, String>() {
//        private String key = val;
//        private String value = val;
//        @Override
//        public String getKey() {
//          return this.key;
//        }
//        @Override
//        public String getValue() {
//          return this.value;
//        }
//        @Override
//        public String setValue(String value) {
//          return this.value = value;
//        }
//        
//      });
//      
//      else secondBatch.add(new Map.Entry<String, String>() {
//        private String key = val;
//        private String value = val;
//        @Override
//        public String getKey() {
//          return this.key;
//        }
//        @Override
//        public String getValue() {
//          return this.value;
//        }
//        @Override
//        public String setValue(String value) {
//          return this.value = value;
//        }
//        
//      });
//    }
//      
//    
//    Thread.sleep(500);
//    
//    verify(cacheLoaderWriter, times(1)).writeAll(firstBatch);
//    
//  }
}
