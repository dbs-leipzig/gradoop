package org.gradoop.flink.io.impl.rdbms;

import java.io.IOException;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.functions.MetadataParser;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSMetadata;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class RDBMSDataSource implements DataSource {
	GradoopFlinkConfig config;
	RDBMSConfig rdbmsconfig;

	public RDBMSDataSource(String url, String user, String pw, GradoopFlinkConfig config) {
		this.config = config;
		this.rdbmsconfig = new RDBMSConfig(url, user, pw);
	}

	@Override
	public LogicalGraph getLogicalGraph() throws IOException {
		// TODO Auto-generated method stub
//		DataSet<RDBMSTable> tables = MetadataParser.parse(RDBMSMetadata.getDBMetaData(ConnectToRDBMS.connect(rdbmsconfig)), config);

		return null;
	}

	@Override
	public GraphCollection getGraphCollection() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
