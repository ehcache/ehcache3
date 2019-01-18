package org.ehcache.clustered.laurentmerckx;

import java.io.Serializable;
import java.util.Map;

public class ProviderArguments implements Serializable {
	private static final long serialVersionUID = 1L;

	// The name of the module to call
	private String moduleName;
	// The version of the module to call
	private String moduleVersion;
	// The requested language code
	private LanguageCode languageCode;
	// The official identifier of the requested enterprise
	private String enterpriseNo;
	// The official identifier of the requested office
	private String officeNo;
	// The global parameters for the provider
	private Map<String,String> globalParameters;
	// The module's parameters
	private Map<String,String> moduleParameters;
	
	public String getModuleName() {
		return moduleName;
	}
	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}
	public String getModuleVersion() {
		return moduleVersion;
	}
	public void setModuleVersion(String moduleVersion) {
		this.moduleVersion = moduleVersion;
	}
	public LanguageCode getLanguageCode() {
		return languageCode;
	}
	public void setLanguageCode(LanguageCode languageCode) {
		this.languageCode = languageCode;
	}
	public String getEnterpriseNo() {
		return enterpriseNo;
	}
	public void setEnterpriseNo(String enterpriseNo) {
		this.enterpriseNo = enterpriseNo;
	}
	public String getOfficeNo() {
		return officeNo;
	}
	public void setOfficeNo(String officeNo) {
		this.officeNo = officeNo;
	}
	public Map<String, String> getGlobalParameters() {
		return globalParameters;
	}
	public void setGlobalParameters(Map<String, String> globalParameters) {
		this.globalParameters = globalParameters;
	}
	public Map<String, String> getModuleParameters() {
		return moduleParameters;
	}
	public void setModuleParameters(Map<String, String> moduleParameters) {
		this.moduleParameters = moduleParameters;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result	+ ((enterpriseNo == null) ? 0 : enterpriseNo.hashCode());
		result = prime * result	+ ((globalParameters == null) ? 0 : globalParameters.hashCode());
		result = prime * result	+ ((languageCode == null) ? 0 : languageCode.hashCode());
		result = prime * result	+ ((moduleName == null) ? 0 : moduleName.hashCode());
		result = prime * result	+ ((moduleParameters == null) ? 0 : moduleParameters.hashCode());
		result = prime * result	+ ((moduleVersion == null) ? 0 : moduleVersion.hashCode());
		result = prime * result	+ ((officeNo == null) ? 0 : officeNo.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProviderArguments other = (ProviderArguments) obj;
		if (enterpriseNo == null) {
			if (other.enterpriseNo != null)
				return false;
		} else if (!enterpriseNo.equals(other.enterpriseNo))
			return false;
		if (globalParameters == null) {
			if (other.globalParameters != null)
				return false;
		} else if (!globalParameters.equals(other.globalParameters))
			return false;
		if (languageCode != other.languageCode)
			return false;
		if (moduleName == null) {
			if (other.moduleName != null)
				return false;
		} else if (!moduleName.equals(other.moduleName))
			return false;
		if (moduleParameters == null) {
			if (other.moduleParameters != null)
				return false;
		} else if (!moduleParameters.equals(other.moduleParameters))
			return false;
		if (moduleVersion == null) {
			if (other.moduleVersion != null)
				return false;
		} else if (!moduleVersion.equals(other.moduleVersion))
			return false;
		if (officeNo == null) {
			if (other.officeNo != null)
				return false;
		} else if (!officeNo.equals(other.officeNo))
			return false;
		return true;
	}

}
