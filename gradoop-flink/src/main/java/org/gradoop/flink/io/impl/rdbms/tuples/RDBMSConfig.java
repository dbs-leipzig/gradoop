package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class RDBMSConfig extends Tuple4<String,String,String,String>{
	public String url;
	public String user;
	public String pw;
	public String jdbcDriverPath;

	public RDBMSConfig(String url, String user, String pw, String jdbcDriverPath) {
		this.url = url;
		this.user = user;
		this.pw = pw;
		this.jdbcDriverPath = jdbcDriverPath;
		this.f0 = url;
		this.f1 = user;
		this.f2 = pw;
		this.f3 = jdbcDriverPath;
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
}
