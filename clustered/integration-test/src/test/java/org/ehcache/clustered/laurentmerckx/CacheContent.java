package org.ehcache.clustered.laurentmerckx;

import java.io.Serializable;
import java.util.Arrays;

public class CacheContent implements Serializable {
	private static final long serialVersionUID = 1L;

	private ContentType contentType;
	private Encoding encoding;
	private byte[] content;
	
	public CacheContent(ContentType contentType, Encoding encoding, byte[] content) {
		this.contentType = contentType;
		this.encoding = encoding;
		this.content = content;
	}

	public ContentType getContentType() {
		return contentType;
	}
	public void setContentType(ContentType contentType) {
		this.contentType = contentType;
	}
	public Encoding getEncoding() {
		return encoding;
	}
	public void setEncoding(Encoding encoding) {
		this.encoding = encoding;
	}
	public byte[] getContent() {
		return content;
	}
	public void setContent(byte[] content) {
		this.content = content;
	}

  @Override
  public int hashCode() {
    return Arrays.hashCode(content);
  }

  @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CacheContent other = (CacheContent) obj;
		if (!Arrays.equals(content, other.content))
			return false;
		if (contentType != other.contentType)
			return false;
		if (encoding != other.encoding)
			return false;
		return true;
	}

}
