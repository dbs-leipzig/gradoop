package org.gradoop.examples.io;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.dot.functions.DOTFileFormat;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.io.impl.rdbms.RDBMSDataSource;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConfig;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class RDBMSExample implements ProgramDescription {
	public static void main(String[] args) throws Exception {
			final String url = args[0];
			final String user = args[1];
			final String pw = args[2];
			final String outputPath = args[3];

			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			GradoopFlinkConfig gfc = GradoopFlinkConfig.createConfig(env);
			
			RDBMSDataSource dataSource = new RDBMSDataSource(url,user,pw,gfc);
			
			LogicalGraph schema = dataSource.getLogicalGraph();
			
			schema.writeTo(new JSONDataSink(outputPath + getDateString() + urlParser(url),gfc));
			
			System.out.println("***JSON Output Graph will write to :" + outputPath + getDateString() + urlParser(url));
			
			env.execute();
    }	
	
	public static String getDateString(){
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date);
	}
	
	public static String urlParser(String url){
		String dbName = url.replaceAll(".*/", "");
		return dbName;
	}

	@Override
	public String getDescription() {
		return "RDBMS to Graph Convertion example";
	}

}
