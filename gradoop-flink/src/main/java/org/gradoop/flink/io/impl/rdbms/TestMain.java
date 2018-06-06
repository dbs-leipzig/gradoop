package org.gradoop.flink.io.impl.rdbms;

import java.sql.Connection;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.rdbms.connect.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConnect;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.MapTypeInformation;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSMetadata;
import org.gradoop.flink.io.impl.rdbms.sequential.MigrationOrder;
import org.gradoop.flink.io.impl.rdbms.sequential.SequentialCycleFinder;
import org.gradoop.flink.io.impl.rdbms.sequential.SequentialMetaDataParser;
import org.gradoop.flink.io.impl.rdbms.sequential.SequentialTablesToEdges;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.SchemaType;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class TestMain{

	public static void main(String[] args) throws Exception {
		final String url = args[0];
		final String user = args[1];
		final String pw = args[2];
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		GradoopFlinkConfig gfc = GradoopFlinkConfig.createConfig(env);
		
		Connection con = RDBMSConnect.connect(rdbmsConfig);

		ArrayList<RDBMSTable> tables = SequentialMetaDataParser.parse(RDBMSMetadata.getDBMetaData(con), con);

		ArrayList<RDBMSTable> toEdges = SequentialTablesToEdges.getTablesToEdges(tables);
		// DataSet<RDBMSTable> dsToEdges = env.fromCollection(toEdges);

		ArrayList<RDBMSTable> toNodes = new MigrationOrder(tables, toEdges).tablesToNodes();
		DataSet<RDBMSTable> dsToNodes = env.fromCollection(new MigrationOrder(tables, toEdges).tablesToNodes());
		
//		RDBMSDataSource dataSource = new RDBMSDataSource(url,user,pw,gfc);
		
//		LogicalGraph schema = dataSource.getLogicalGraph();
		
//		schema.writeTo(new JSONDataSink("/home/pc/01 Uni/8. Semester/Bachelorarbeit/Test_Outputs/Test_Graph",gfc));
		
//		env.execute();
		
		
		// **Sequential Section**
		// TesDatabases : smalltest, employees, cycleTest, longCycleTest,
		// longCycleTestRev

//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		GradoopFlinkConfig gfc = GradoopFlinkConfig.createConfig(env);
//
//		LogicalGraphFactory lgf = new LogicalGraphFactory(gfc);
//		lgf.setLayoutFactory(new GVEGraphLayoutFactory());
//		VertexFactory vertexFactory = gfc.getVertexFactory();
//
//		RDBMSConfig rdbmsconfig = new RDBMSConfig(RDBMSConstants.URL, RDBMSConstants.USER, RDBMSConstants.PW);
//		Connection con = RDBMSConnect.connect(rdbmsconfig);
//		ArrayList<RDBMSTable> tables = SequentialMetaDataParser
//				.parse(RDBMSMetadata.getDBMetaData(con),con);

		// ***sequential section
//
//		ArrayList<RDBMSTable> toEdges = SequentialTablesToEdges.getTablesToEdges(tables);
//
//		ArrayList<RDBMSTable> toNodes = new MigrationOrder(tables, toEdges).tablesToNodes();

		// HashSet<RDBMSTable> cyclicTables = new
		// SequentialCycleFinder(tables).findAllCycles();
		// DataSet<RDBMSTable> dsToNodes = env.fromCollection(toNodes);
		// LogicalGraph lg = new TuplesToNodes().getToNodesGraph(lgf);

//		System.out.println("\n***TABLE STRUCTURE***\n");
//		for (RDBMSTable table : tables) {
//			System.out.println(table.getTableName());
//			for(SchemaType col : table.getPrimaryKey()){
//				System.out.println(col.getName() + " " + col.getPos());
//			}
//			if (table.getForeignKeys() != null) {
//				Iterator it = table.getForeignKeys().entrySet().iterator();
//				while (it.hasNext()) {
//					Map.Entry<SchemaType,String> pair = (Map.Entry) it.next();
//					System.out.println(pair.getKey().getName() + " " + pair.getKey().getPos() + " --> " + pair.getValue());
//				}
//			}
//			Iterator it = table.getAttributes().entrySet().iterator();
//			while (it.hasNext()) {
//				Map.Entry<SchemaType,JDBCType> pair = (Map.Entry) it.next();
//				System.out.println(pair.getKey().getName()+ " " + pair.getKey().getPos() + " : " + pair.getValue());
//			}
//			System.out.println("No. of Rows: "+table.getNumberOfRows());
//			System.out.println(".........................................");
//		}
//
//		System.out.println("\n***FIND TABLES TO EDGES***\n");
//		for (RDBMSTable r : toEdges) {
//			System.out.println(r.getTableName());
//		}
//
//		// System.out.println("\n***FIND CYCLIC TABLES***\n");
//		// for (RDBMSTable table : cyclicTables) {
//		// System.out.println(table.getTableName());
//		// }
//		//
//		System.out.println("\n***FIND TABLES TO NODES***\n");
//		for (RDBMSTable tn : toNodes) {
//			System.out.println(tn.getTableName());
//		}
//
////		LogicalGraph toNodesGraph = new TuplesToNodes().getToNodesGraph(env, lgf, toNodes);
//		DataSet<Row> flinkTable = new FlinkConnect().connect(env, tables.get(4));
//		DataSet<Tuple2<String,String>> first = flinkTable.map( new TestMapper());
//		first.print();
//		flinkTable.writeAsText("/home/pc/01 Uni/8. Semester/Bachelorarbeit/Test_Outputs",WriteMode.OVERWRITE);
//		System.out.println(env.getParallelism());
//		env.execute();
	}
}
