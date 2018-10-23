package csw.spark.s2;

public class AppInfo {

	public AppInfo(String appId, String platform, String url) {
		super();
		this.appId = appId;
		this.platform = platform;
		this.url = url;
	}

	private String appId;
	private String platform;
	private String url;


	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

}
