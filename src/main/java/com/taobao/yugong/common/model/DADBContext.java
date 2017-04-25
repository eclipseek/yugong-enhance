package com.taobao.yugong.common.model;

/**
 * Created by zhangyq on 2016-11-04.
 */
public class DADBContext {
	private String driver;
	private String url;
	private String user;
	private String password;

	public String getDriver() {
		return driver;
	}

	public DADBContext setDriver(String driver) {
		this.driver = driver;
		return this;
	}

	public String getUrl() {
		return url;
	}

	public DADBContext setUrl(String url) {
		this.url = url;
		return this;
	}

	public String getUser() {
		return user;
	}

	public DADBContext setUser(String user) {
		this.user = user;
		return this;
	}

	public String getPassword() {
		return password;
	}

	public DADBContext setPassword(String password) {
		this.password = password;
		return this;
	}
}
