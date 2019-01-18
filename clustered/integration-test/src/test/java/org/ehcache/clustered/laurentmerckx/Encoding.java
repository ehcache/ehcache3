package org.ehcache.clustered.laurentmerckx;

/**
 * Encoding (character-sets) supported by DPS.
 * 
 * @author MERCKX Laurent
 */
public enum Encoding {

	LATIN1("ISO-8859-1"),
	UTF8("UTF-8");
	
	// Default encoding is UTF-8
	public final static Encoding DEFAULT = UTF8;
	
	// Corresponding official code of encoding
	private String encodingCode;
	
	Encoding(String encodingCode) {
		this.encodingCode = encodingCode;
	}

	/**
	 * Returns the corresponding official code of encoding
	 * @return The corresponding official code of encoding
	 */
	public String getEncodingCode() {
		return encodingCode;
	}
	
	/**
	 * Returns the encoding corresponding to specified code
	 * @param code The code of the encoding to find
	 * @return The corresponding encoding object or null if not found
	 */
	public static Encoding fromString(String code) {
		for (Encoding encoding: values()) {
			if (encoding.encodingCode.equalsIgnoreCase(code))
				return encoding;			
		}
		return null;
	}
	
}
