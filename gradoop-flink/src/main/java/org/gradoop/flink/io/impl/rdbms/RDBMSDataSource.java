package org.gradoop.flink.io.impl.rdbms;

import java.io.IOException;

import org.apache.flink.api.common.io.InputFormat;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class RDBMSDataSource implements DataSource{
	
	private String driver;
	private String url;
	private GradoopFlinkConfig config;
	
	public RDBMSDataSource(String driver, String url, GradoopFlinkConfig config) {
		this.driver = driver;
		this.url = url;
		this.config = config;
	  }

	@Override
	public LogicalGraph getLogicalGraph() throws IOException {
		// TODO Auto-generated method stub
		
		
		return null;
	}

	@Override
	public GraphCollection getGraphCollection() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
