package org.ehcache.clustered.laurentmerckx;

/**
 * Language supported by DPS.
 * 
 * @author MERCKX Laurent
 */
public enum LanguageCode {
	
	FRENCH("fr","1"),
	ENGLISH("en","5"),
	DUTCH("nl","3"),
	GERMAN("de","2"),
	_COMPANY("_C","C"); // FIXME: Not supported by API
	
	// Default language is English
	public final static LanguageCode DEFAULT = ENGLISH;
	
	// Corresponding official ISO code of language
	private String isoCode;
	// Corresponding internal code of language
	private String internalCode;
	
	LanguageCode(String isoCode, String internalCode) {
		this.isoCode = isoCode;
		this.internalCode = internalCode;
	}

	/**
	 * Returns the corresponding official ISO code of language
	 * @return The corresponding official ISO code of language
	 */
	public final String getIsoCode() {
		return isoCode;
	}

	/**
	 * Returns the corresponding internal code of language
	 * @return The corresponding internal code of language
	 */
	public final String getInternalCode() {
		return internalCode;
	}
	
	/**
	 * Returns the language corresponding to specified code (ISO or internal code)
	 * @param code The ISO or internal code of the language to find
	 * @return The corresponding language object or null if not found
	 */
	public static LanguageCode fromString(String code) {
		for (LanguageCode languageCode: values()) {
			if (languageCode.isoCode.equalsIgnoreCase(code) || languageCode.internalCode.equals(code))
				return languageCode;			
		}
		return null;
	}
		
}
