package org.ehcache.impl.config.persistence;

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
	private static final String CACHE_ALIAS = "bugReportCache";
	private static final String XML_CONFIG_PATH
		= "/org/ehcache/impl/config/persistence/ehcache-dir-persistence-test.xml";

	/**
	 * For restoration of cache after process termination this has to be false so that Java API
	 * config is used.
	 * Then, the following file is persisted when manager is closed, which allows to restart a
	 * cache with previous
	 * mappings:
	 * -rw-rw-r-- 1 resat resat	697 Fev 10 20:29 ehcache-disk-store.index
	 * If the data is recovered, the following is logged:
	 * Feb 10, 2021 8:41:15 PM org.ehcache.impl.internal.store.disk.OffHeapDiskStore
	 *  recoverBackingMap
			INFO: The index for data file ehcache-disk-store.data is more recent than the data file
				itself by 345ms : this is harmless.
	 */
	protected static final boolean USE_XML_BASED_CONFIG = false;

	private PersistentCacheManager ehCacheManager;
	private Cache<String, String> cache;

	static String getStoragePath() {
		return System.getProperty("user.home") + PERSISTENCE_DIRECTORY;
	}

	static PersistentCacheManager initCacheManagerFromXMLConfig() {
		URL myUrl = EhCacheProviderWithXMLOrJavaBasedConfig.class.getResource(XML_CONFIG_PATH);
		XmlConfiguration xmlConfig = new XmlConfiguration(myUrl); 
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

	EhCacheProviderWithXMLOrJavaBasedConfig() {
		this(USE_XML_BASED_CONFIG);
	}

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
