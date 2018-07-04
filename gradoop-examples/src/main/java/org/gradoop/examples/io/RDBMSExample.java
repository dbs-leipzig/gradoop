package org.gradoop.examples.io;


import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;
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
import org.gradoop.flink.io.impl.rdbms.connection.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing.TEMPDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
/**
 * Example program that converts a given relational database 
 * into a {@link LogicalGraph} and stores the
 * resulting {@link LogicalGraph} as JSON into specified directory.
 * 
 * 
 */
public class RDBMSExample implements ProgramDescription {
	/**
	   * Reads an EPGM logical graph from a relational database.
	   *
	   * args[0]: jdbc url with standard format jdbc:[management system]://[port]/[database name] (e.g. jdbc:mysql://localhost/employees)
	   * args[1]: user name of database
	   * args[2]: password of database
	   * args[3]: output directory of JSON files representing the converted EPGM
	   *
	   * @param args program arguments
	   */
	public static void main(String[] args) throws Exception {
		if(args.length != 5){
			 throw new IllegalArgumentException(
				        "provide url, user, pasword, output directory");
		}
			final String url = args[0];
			final String user = args[1];
			final String pw = args[2];
			final String jdbcDriverPath = args[3];
			final String jdbcDriverClassName = args[4];
			final String outputPath = args[5];

		    // init Flink execution environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
		    // create default Gradoop config
			GradoopFlinkConfig gfc = GradoopFlinkConfig.createConfig(env);
			
		    // create DataSource
			RDBMSDataSource dataSource = new RDBMSDataSource(url,user,pw,jdbcDriverPath, jdbcDriverClassName,gfc);
		    
			// get logical graph of datasource
			LogicalGraph schema = dataSource.getLogicalGraph();
			
			// write conversion result to given path with timestamp and db name
			schema.writeTo(new JSONDataSink(outputPath + getDateString() + urlParser(url),gfc));
			
			// execute program
			env.execute();
			
//			JobExecutionResult result = env.execute();
//			PrintWriter printer = new PrintWriter(new FileWriter("/home/pc/01 Uni/8. Semester/Bachelorarbeit/Out/Time/"+getDateString()+"time"));
//			printer.write(String.valueOf(result.getNetRuntime(TimeUnit.MILLISECONDS)));
//			printer.close();
    }	
	
	/*
	 * create timestamp
	 */
	public static String getDateString(){
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date);
	}
	
	/*
	 * create db name 
	 */
	public static String urlParser(String url){
		String dbName = url.replaceAll(".*/", "");
		return dbName;
	}

	@Override
	public String getDescription() {
		return "RDBMS to Graph Convertion example";
	}

}
