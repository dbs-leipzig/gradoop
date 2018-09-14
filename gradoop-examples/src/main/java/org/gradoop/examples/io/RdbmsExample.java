/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.examples.io;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.rdbms.RdbmsDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Example program that converts a given relational database 
 * into a {@link LogicalGraph} and stores the
 * resulting {@link LogicalGraph} as JSON into declared directory.
 */
public class RdbmsExample implements ProgramDescription {

	/**
	 * Converts a relational database to an epgm graph
	 *
	 * @param args[0] Jdbc url 
	 * @param args[1] Username of database user
	 * @param args[2] Password of database user
	 * @param args[3] Valid path to a fitting jdbc driver
	 * @param args[4] Valid jdbc driver class name
	 * @param args[5] Valid path to output directory
	 */
	public static void main(String[] args) throws Exception {

		if(args.length != 6){
			throw new IllegalArgumentException(
					"Please provide url, user, pasword, path to jdbc driver jar, jdbc driver class name, output directory");
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
		RdbmsDataSource dataSource = new RdbmsDataSource(url,user,pw,jdbcDriverPath, jdbcDriverClassName,gfc);

		// get logical graph of datasource
		LogicalGraph schema = dataSource.getLogicalGraph();

		// write conversion result to given path with timestamp and db name
		schema.writeTo(new CSVDataSink(outputPath + "/" + getDateString() + urlParser(url),gfc));
		
		// execute program
		env.execute();
	}	
	
	/**
	 * Creates timestamp for output filename
	 * @return Timestamp
	 */
	public static String getDateString(){
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date);
	}

	/**
	 * Provides the name of the connected database
	 * @return Database name
	 */
	public static String urlParser(String url){
		String dbName = url.replaceAll(".*/", "");
		return dbName;
	}

	@Override
	public String getDescription() {
		return "DataImport for relational databases, implementing a relational database to epgm graph database conversation.";
	}

}
