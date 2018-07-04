package org.gradoop.flink.io.impl.rdbms.connection;

public class RDBMSConfig {
	private String url;
	private String user;
	private String pw;
	private String jdbcDriverPath;
	private String jdbcDriverClassName;

	public RDBMSConfig(String url, String user, String pw, String jdbcDriverPath, String jdbcDriverClassName) {
		this.url = url;
		this.user = user;
		this.pw = pw;
		this.jdbcDriverPath = jdbcDriverPath;
		this.jdbcDriverClassName = jdbcDriverClassName;
	}


	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPw() {
		return pw;
	}

	public void setPw(String pw) {
		this.pw = pw;
	}

	public String getJdbcDriverPath() {
		return jdbcDriverPath;
	}

	public void setJdbcDriverPath(String jdbcDriverPath) {
		this.jdbcDriverPath = jdbcDriverPath;
	}

	public String getJdbcDriverClassName() {
		return jdbcDriverClassName;
	}

	public void setJdbcDriverClassName(String jdbcDriverClassName) {
		this.jdbcDriverClassName = jdbcDriverClassName;
	}
}
