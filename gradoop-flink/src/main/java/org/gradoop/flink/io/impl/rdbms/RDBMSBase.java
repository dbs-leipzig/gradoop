package org.gradoop.flink.io.impl.rdbms;

import java.util.Objects;

import org.gradoop.flink.util.GradoopFlinkConfig;

public abstract class RDBMSBase {
	private String url;
	private String user;
	private String pasword;
	private GradoopFlinkConfig config;
	
	protected RDBMSBase(String url, String user, String pasword, GradoopFlinkConfig config){
		this.url = Objects.requireNonNull(url);
		this.user = user;
		this.pasword = pasword;
		this.config = Objects.requireNonNull(config);
	}
}
