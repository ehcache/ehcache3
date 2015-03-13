package org.ehcache.demos.peeper;

import org.ehcache.StandaloneCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.ehcache.StandaloneCacheBuilder.newCacheBuilder;

/**
 * Created by shdi on 3/12/15.
 */
public class DataCacheAside {

    private StandaloneCache<String, List> cache;
    private static final Logger logger = LoggerFactory.getLogger(DataCacheAside.class);

    public void setupCache() {
        cache = newCacheBuilder(String.class, List.class, LoggerFactory.getLogger(getClass())).build();
        cache.init();
        logger.info("Cache setup is done");
    }

    public List getFromCache() {
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
