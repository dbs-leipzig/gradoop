package org.gradoop.flink.io.impl.rdbms.connection;

/**
 * Stores database management system parameters
 */
public class RdbmsConfig {
	
	/**
	 * Valid jdbc url
	 */
	private String url;
	
	/**
	 * Username of database management system user
	 */
	private String user;
	
	/**
	 * Password of database management system user
	 */
	private String pw;
	
	/**
	 * Valid path to a fitting jdbc driver jar
	 */
	private String jdbcDriverPath;
	
	/**
	 * Valid and fitting jdbc driver class name 
	 */
	private String jdbcDriverClassName;

	/**
	 * Construcotr
	 * @param url Valid jdbc url
	 * @param user Username of database management system user
	 * @param pw Password of database management system user
	 * @param jdbcDriverPath Valid path to a fitting jdbc driver jar
	 * @param jdbcDriverClassName Valid and fitting jdbc driver class name
	 */
	public RdbmsConfig(String url, String user, String pw, String jdbcDriverPath, String jdbcDriverClassName) {
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
