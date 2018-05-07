package org.gradoop.flink.io.impl.rdbms;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConnect;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.FindCycleTable;
import org.gradoop.flink.io.impl.rdbms.functions.MetadataParser;
import org.gradoop.flink.io.impl.rdbms.functions.TablesToEdgesCount;
import org.gradoop.flink.io.impl.rdbms.functions.TablesToEdgesFilter;
import org.gradoop.flink.io.impl.rdbms.functions.ToCycleTuple;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSMetadata;
import org.gradoop.flink.io.impl.rdbms.sequential.MigrationOrder;
import org.gradoop.flink.io.impl.rdbms.sequential.SequentialCycleFinder;
import org.gradoop.flink.io.impl.rdbms.sequential.SequentialMetaDataParser;
import org.gradoop.flink.io.impl.rdbms.sequential.SequentialTablesToEdges;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class TestMain {

	public static void main(String[] args) throws Exception {

		// **Sequential Section**
		// TesDatabases : smalltest, employees, cycleTest, longCycleTest, longCycleTestRev

		RDBMSConfig rdbmsconfig = new RDBMSConfig("jdbc:mysql://localhost/longCycleTestRev", RDBMSConstants.USER,
				RDBMSConstants.PW);
		ArrayList<RDBMSTable> tables = SequentialMetaDataParser
				.parse(RDBMSMetadata.getDBMetaData(RDBMSConnect.connect(rdbmsconfig)));

		ArrayList<String> toEdges = SequentialTablesToEdges.getTablesToEdges(tables);

		ArrayList<String> toNodes = new MigrationOrder(tables, toEdges).tablesToNodes();

		HashSet<String> cyclicTables = new SequentialCycleFinder(tables).findAllCycles();

		System.out.println("\n***TABLE STRUCTURE***\n");
		for (RDBMSTable table : tables) {
			System.out.println(table.getTableName() + " | " + table.getPrimaryKey() + " | ");
			if (table.getForeignKeys() != null) {
				Iterator it = table.getForeignKeys().entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					System.out.println(pair.getKey() + " --> " + pair.getValue());
				}
			}
			System.out.println(".........................................");
		}
		System.out.println("\n***FIND TABLES TO EDGES***\n");
		for (String r : toEdges) {
			System.out.println(r);
		}
		System.out.println("\n***FIND CYCLIC TABLES***\n");
		for (String table : cyclicTables) {
			System.out.println(table);
		}

		System.out.println("\n***FIND TABLES TO NODES***\n");

		for (String tn : toNodes) {
			System.out.println(tn);
		}

		// **Flink Section**

		// ExecutionEnvironment env =
		// ExecutionEnvironment.getExecutionEnvironment();
		// GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
		// RDBMSConfig rdbmsconfig = new
		// RDBMSConfig("jdbc:mysql://localhost/employees","hr73vexy","UrlaubsReisen");
		// DataSet<Tuple4<String,String,String,String>> tables =
		// MetadataParser.parse(RDBMSMetadata.getDBMetaData(RDBMSConnect.connect(rdbmsconfig)),
		// config);
		// DataSet<Tuple1<String>> tablesToEdges = tables.flatMap(new
		// TablesToEdgesCount()).groupBy(0).sum(1).filter(new
		// TablesToEdgesFilter()).project(0);
		// DataSet<Tuple2<String,String>> temp = tables.project(0,3);
		// DataSet<Tuple3<String,Integer,Integer>> tablesCyclic =
		// temp.flatMap(new ToCycleTuple());
		//
		// total.print();

		// tablesToEdges.writeAsText("/home/pc/01 Uni/8.
		// Semester/Bachelorarbeit/Test_Outputs",WriteMode.OVERWRITE);
		// env.execute();
	}
}
