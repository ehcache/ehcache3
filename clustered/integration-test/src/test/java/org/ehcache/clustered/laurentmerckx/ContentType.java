package org.ehcache.clustered.laurentmerckx;

/**
 * Content-types supported by DPS.
 * 
 * @author MERCKX Laurent
 */
public enum ContentType {
	
	XML("application/xml"),
	JSON("application/json");

	// Default content-type is JSON
	public final static ContentType DEFAULT = JSON;

	// Corresponding Mime-code
	private String mimeType;
	
	ContentType(String mimeType) {
		this.mimeType = mimeType;
	}

	/**
	 * Returns the corresponding mime-code
	 * @return The corresponding mime-code
	 */
	public String getMimeType() {
		return mimeType;
	}
	
	/**
	 * Returns the content-type corresponding to specified code
	 * @param code The code of the content-type to find
	 * @return The corresponding content-type object or null if not found
	 */
	public static ContentType fromString(String code) {
		for (ContentType contentType: values()) {
			if (contentType.mimeType.equalsIgnoreCase(code))
				return contentType;			
		}
		return null;
	}
	
}
