package org.ehcache.xml.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * @author Reşat SABIQ
 */
@RunWith(Parameterized.class)
public class EhCacheProviderWithXMLOrJavaBasedConfigTest {
	private static final String[][] KEY_VALUE_PAIRS = new String[][]
			{{"Astronaut", "Fezacı"}, {"Cosmonaut", "Fezagir"}};
	private static final String SPACE = " ";

	private boolean useXmlBasedConfig;
	private boolean assertRecoveryFromDisk;

	private EhCacheProviderWithXMLOrJavaBasedConfig ehCacheProvider;


	static Stream<Path> getPersistenceDirectoryListing(String subfolder) throws IOException {
		String path = EhCacheProviderWithXMLOrJavaBasedConfig.getStoragePath();
		if (subfolder != null)
			path += subfolder;
		return Files.list(Paths.get(path));
	}

	@Parameters(name = "{index}: xmlBasedConfig = {0}, recoveryFromDisk = {1}")
	public static Collection<Boolean[]> data() {
		return Arrays.asList(
			new Boolean[][] {
				{Boolean.TRUE, Boolean.FALSE},	{Boolean.TRUE, Boolean.TRUE},
				{Boolean.FALSE, Boolean.FALSE},	{Boolean.FALSE, Boolean.TRUE}
			}
		);
	}

	@After
	public void afterEach() throws IOException {
		PersistentCacheManager cm = ehCacheProvider.getEhCacheManager();
		cm.close();
		assertTrue(getPersistenceDirectoryListing(null).count() > 0);
		long fileDirListingSize = getPersistenceDirectoryListing("/file").count();
		assertTrue("disk persistence directory shouldn't have been empty", fileDirListingSize > 0);
	}

	/**
	 * @param useXmlBasedConfig	true to use XML-based config, false to use Java-based one
	 * @param recoveryFromDisk	true to assert successful recovery from disk, false otherwise
	 */
	public EhCacheProviderWithXMLOrJavaBasedConfigTest(Boolean xmlBasedConfig, Boolean recoveryFromDisk) {
		this.useXmlBasedConfig = xmlBasedConfig;
		this.assertRecoveryFromDisk = recoveryFromDisk;
	}

	@Test
	public final void testPutAndGet() {
		doPutAndGet();
	}

	private final void doPutAndGet() {
		ehCacheProvider = new EhCacheProviderWithXMLOrJavaBasedConfig(useXmlBasedConfig);
		Cache<String, String> cache = ehCacheProvider.getEhCache();

		if (assertRecoveryFromDisk)
			assertEquals("cache data should've been recovered from disk",
					KEY_VALUE_PAIRS[1][1], cache.get(KEY_VALUE_PAIRS[1][0]));

		// put:
		cache.put(KEY_VALUE_PAIRS[0][0], KEY_VALUE_PAIRS[0][1]);
		cache.put(KEY_VALUE_PAIRS[1][0], KEY_VALUE_PAIRS[1][1]);

		// and get:
		cache.get(KEY_VALUE_PAIRS[0][0]);
		cache.get(KEY_VALUE_PAIRS[1][0]);

		assertEquals(KEY_VALUE_PAIRS[0][1], cache.get(KEY_VALUE_PAIRS[0][0]));
	}
}
