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
import java.util.logging.Logger;
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
	private static final Logger logger = Logger.getLogger(EhCacheProviderWithXMLOrJavaBasedConfigTest.class.getName());
	private static final String[][] KEY_VALUE_PAIRS = new String[][]
			{{"Astronaut", "Fezacı"}, {"Cosmonaut", "Fezagir"}};
	private static final String SPACE = " ";

	private boolean useXmlBaseConfig;
	private EhCacheProviderWithXMLOrJavaBasedConfig ehCacheProvider;

	static Stream<Path> getPersistenceDirectoryListing(String subfolder) throws IOException {
		String path = EhCacheProviderWithXMLOrJavaBasedConfig.getStoragePath();
		if (subfolder != null)
			path += subfolder;
		return Files.list(Paths.get(path));
	}


	@Parameters(name = "{index}: useXmlBasedConfig = {0}")
	public static Collection<Boolean> data() {
		return Arrays.asList(
			Boolean.TRUE, /* comment out this line & run (twice) to check 
						  /*	OffHeapDiskStore recoverBackingMap functionality */
			Boolean.FALSE);
	}

	@After
	public void afterEach() throws IOException, InterruptedException {
		PersistentCacheManager cm = ehCacheProvider.getEhCacheManager();
		cm.close();
		assertTrue(getPersistenceDirectoryListing(null).count() > 0);
		long fileDirListingSize = getPersistenceDirectoryListing("/file").count();
		if (logger.isLoggable(Level.INFO))
			logger.info("fileDirListingSize: " +fileDirListingSize);
		assertTrue(fileDirListingSize > 0);
	}

	/**
	 * @param useXmlBaseConfig
	 */
	public EhCacheProviderWithXMLOrJavaBasedConfigTest(Boolean useXmlBaseConfig) {
		this.useXmlBaseConfig = useXmlBaseConfig;
	}

	@Test
	public final void testEhCachePutAndGet() {
		doEhCachePutAndGet();
	}

	private final void doEhCachePutAndGet() {
		ehCacheProvider = new EhCacheProviderWithXMLOrJavaBasedConfig(useXmlBaseConfig);
		Cache<String, String> cache = ehCacheProvider.getEhCache();
		if (logger.isLoggable(Level.WARNING))
			logger.warning("On 2nd execution using the same config type,"
				" true with Java-based config, false with XML-based one: "
					+cache.containsKey(KEY_VALUE_PAIRS[1][0])
					+ " (" +cache.get(KEY_VALUE_PAIRS[1][0])+ ')');

		// put:
		cache.put(KEY_VALUE_PAIRS[0][0], KEY_VALUE_PAIRS[0][1]);
		cache.put(KEY_VALUE_PAIRS[1][0], KEY_VALUE_PAIRS[1][1]);

		String filesList = null;
		try {
			filesList = getPersistenceDirectoryListing(null).map(p -> p.getFileName())
				.map(Object::toString).collect(Collectors.joining(SPACE));
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (logger.isLoggable(Level.INFO))
			logger.info("Put 2 entries into: " +cache+ ". Files: " +filesList);

		// and get:
		cache.get(KEY_VALUE_PAIRS[0][0]);
		cache.get(KEY_VALUE_PAIRS[1][0]);

		if (logger.isLoggable(Level.INFO))
			logger.info("Got 2 entries: " +cache.containsKey(KEY_VALUE_PAIRS[1][0]));

		assertEquals(KEY_VALUE_PAIRS[0][1], cache.get(KEY_VALUE_PAIRS[0][0]));
	}
}
