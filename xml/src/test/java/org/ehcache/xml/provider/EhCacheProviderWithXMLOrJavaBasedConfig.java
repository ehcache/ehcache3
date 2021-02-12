package org.ehcache.xml.provider;

import java.io.File;
import java.net.URL;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.XmlConfiguration;

/**
 * @author Reşat SABIQ
 */
public class EhCacheProviderWithXMLOrJavaBasedConfig {
	private static final String PERSISTENCE_DIRECTORY = "/temp/ehcache";
	private static final String CACHE_ALIAS = "testCache";
	private static final String XML_CONFIG_PATH
		= "/configs/disk-persistent-cache-with-heap-of-1-entry.xml";

	private PersistentCacheManager ehCacheManager;
	private Cache<String, String> cache;

	static String getStoragePath() {
		return System.getProperty("user.home") + PERSISTENCE_DIRECTORY;
	}

	static PersistentCacheManager initCacheManagerFromXMLConfig() {
		URL url = EhCacheProviderWithXMLOrJavaBasedConfig.class.getResource(XML_CONFIG_PATH);
		XmlConfiguration xmlConfig = new XmlConfiguration(url); 
		return (PersistentCacheManager)CacheManagerBuilder.newCacheManager(xmlConfig);
	}

	static PersistentCacheManager initCacheManagerUsingJavaAPI() {
		PersistentCacheManager persistentCacheManager = 
			CacheManagerBuilder.newCacheManagerBuilder()
				.with(CacheManagerBuilder.persistence(getStoragePath()+ File.separator))
				.withCache(CACHE_ALIAS, CacheConfigurationBuilder
					.newCacheConfigurationBuilder(String.class, String.class,
						ResourcePoolsBuilder.newResourcePoolsBuilder()
							.heap(1, EntryUnit.ENTRIES)
							.disk(10, MemoryUnit.MB, true)) 
				)
				.build(true);
		return persistentCacheManager;
	}

	/*
	 * Construct using Java-API-based config
	 */
	EhCacheProviderWithXMLOrJavaBasedConfig() {
		this(false);
	}

	/**
	 * @param	useXmlBasedConfig	true to use XML-based config, false to use Java-API-based one
	 */
	EhCacheProviderWithXMLOrJavaBasedConfig(boolean useXmlBasedConfig) {
		if (useXmlBasedConfig) {
			ehCacheManager = initCacheManagerFromXMLConfig();
			ehCacheManager.init();
		} else
			ehCacheManager = initCacheManagerUsingJavaAPI();
		cache = ehCacheManager.getCache(CACHE_ALIAS, String.class, String.class);
	}

	public Cache<String, String> getEhCache() {
		return cache;
	}

	public PersistentCacheManager getEhCacheManager() {
		return ehCacheManager;
	}

	@Override
	protected void finalize() throws Throwable {
		ehCacheManager.close();
	}
}
