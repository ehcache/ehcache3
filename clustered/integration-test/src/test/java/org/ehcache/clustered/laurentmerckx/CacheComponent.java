package org.ehcache.clustered.laurentmerckx;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.xml.XmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import static org.ehcache.clustered.CacheManagerLifecycleEhcacheIntegrationTest.substitute;

public class CacheComponent {
	
	private static final Logger logger = LoggerFactory.getLogger(CacheComponent.class);
	
	private CacheManager cacheManager;
	private Cache<ProviderArguments, CacheContent> cache;
	

	public CacheComponent(URI server) throws IOException {
    URL resource = CacheComponent.class.getResource("/configs/laurentmerckx-config.xml");
    URL substitutedResource = substitute(resource, "cluster-uri", server.toString());

    Configuration xmlConfig = new XmlConfiguration(substitutedResource);
		cacheManager = CacheManagerBuilder.newCacheManager(xmlConfig);
		cacheManager.init();
		cache = cacheManager.getCache("test-cache", ProviderArguments.class, CacheContent.class);
	}
	
	public CacheContent get(ProviderArguments arguments) {
		CacheContent cacheContent = cache.get(arguments);
		return cacheContent;
	}
	
	public void put(ProviderArguments arguments, CacheContent content) {
		cache.put(arguments, content);
	}
	
	public void clear() {
		cache.clear();
	}

}
