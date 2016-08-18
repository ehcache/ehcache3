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
package org.ehcache.demos.peeper;

import org.ehcache.UserManagedCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.ehcache.config.builders.UserManagedCacheBuilder.newUserManagedCacheBuilder;

/**
 * Created by shdi on 3/12/15.
 */
public class DataCache {

    private UserManagedCache<String, List> cache;
    private static final Logger logger = LoggerFactory.getLogger(DataCache.class);

    public void setupCache() {
        cache = newUserManagedCacheBuilder(String.class, List.class).identifier("data-cache").build(true);
        logger.info("Cache setup is done");
    }

    public List<String> getFromCache() {
        return cache.get("all-peeps");
    }

    public void addToCache(List<String> line) {
        cache.put("all-peeps", line);
    }

    public void clearCache() {
        cache.clear();
    }

    public void close() {
        cache.close();
    }

}
