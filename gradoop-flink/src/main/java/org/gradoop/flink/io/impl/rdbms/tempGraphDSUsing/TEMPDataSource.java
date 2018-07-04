package org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.rdbms.connection.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connection.RDBMSConnect;
import org.gradoop.flink.io.impl.rdbms.metadata.MetaDataParser;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSTableBase;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSConfig;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class TEMPDataSource implements DataSource {

	// standard gradoop flink config
	private GradoopFlinkConfig config;

	// config storing rdbms' url,user,pw informations
	private RDBMSConfig rdbmsConfig;

	// standard flink execution environment
	private ExecutionEnvironment env;

	/**
	 * Transforms a relational database with given parameters into an EPGM
	 * database
	 * 
	 * @param url
	 *            jdbc standard url - jdbc:[managementsystem
	 *            identifier]://[host:port]/[databasename] (e.g.
	 *            jdbc:mysql://localhost/employees)
	 * @param user
	 *            username of database
	 * @param pw
	 *            password of database
	 * @param config
	 *            standard GradoopFlinkConfig
	 */
	public TEMPDataSource(String url, String user, String pw, String jdbcDriverPath, GradoopFlinkConfig config) {
		this.config = config;
		this.rdbmsConfig = new RDBMSConfig(url, user, pw, jdbcDriverPath);
		this.env = config.getExecutionEnvironment();
	}

	@Override
	public LogicalGraph getLogicalGraph() {

		/*
		 * connection to rdbms via jdbc
		 */
		Connection con = RDBMSConnect.connect(rdbmsConfig);

		DataSet<ImportVertex<String>> vertices = null;
		DataSet<ImportEdge<String>> edges = null;

		try {

			MetaDataParser metadata = new MetaDataParser(con);
			metadata.parse();

			/*
			 * tables convert to nodes
			 */
			ArrayList<TablesToNodes> tablesToNodes = metadata.getTablesToNodes();

			/*
			 * tables convert to edges
			 */
			ArrayList<TablesToEdges> tablesToEdges = metadata.getTablesToEdges();

			System.out.println("to Nodes :\n");
			for (TablesToNodes table : tablesToNodes) {
				System.out.println(table.getTableName());
				System.out.println(table.getSqlQuery());
			}
			System.out.println("to Edges :\n");
			for (TablesToEdges table : tablesToEdges) {
				System.out.println(table.getRelationshipType());
				System.out.println(table.getSqlQuery());
			}

			/*
			 * converts rdbms table tuples to vertices
			 */
			int tablePos = 0;
			for (TablesToNodes table : tablesToNodes) {
				DataSet<Row> dsSQLResult = new FlinkConnect().connect(env, rdbmsConfig, table.getRowCount(),
						table.getSqlQuery(), table.getRowTypeInfo());
				if (tablePos == 0) {
					vertices = dsSQLResult.map(new TupleToVertex(tablePos))
							.withBroadcastSet(env.fromCollection(tablesToNodes), "tables");
				} else {
					vertices = vertices.union(dsSQLResult.map(new TupleToVertex(tablePos))
							.withBroadcastSet(env.fromCollection(tablesToNodes), "tables"));
				}
				tablePos++;
			}

			/*
			 * converts rdbms table tuples to edges
			 */
			tablePos = 0;
			for (TablesToEdges table : tablesToEdges) {
				DataSet<Row> dsSQLResult = FlinkConnect.connect(env, rdbmsConfig, table.getRowCount(),
						table.getSqlQuery(), table.getRowTypeInfo());
				if (table.isDirectionIndicator()) {
					if (tablePos == 0) {
						edges = dsSQLResult.map(new FkToEdge(tablePos))
								.withBroadcastSet(env.fromCollection(tablesToEdges), "tables");
					} else {
						edges = edges.union(dsSQLResult.map(new FkToEdge(tablePos))
								.withBroadcastSet(env.fromCollection(tablesToEdges), "tables"));
					}
				} else {
					if(tablePos == 0){
						edges = dsSQLResult.map(new TupleToEdge(tablePos)).withBroadcastSet(env.fromCollection(tablesToEdges), "tables");
					}else{
						
					}
					edges = edges.union(dsSQLResult.map(new TupleToEdge(tablePos)).withBroadcastSet(env.fromCollection(tablesToEdges), "tables"));
				}
				tablePos++;
			}
			// /*
			// * convert rdbms table's tuples to nodes
			// */
			// int tnPos = 0;
			// for (RDBMSTable table : tablesToNodes) {
			//
			// //queryresult of table querying primary keys, foreign keys and
			// attributes
			// DataSet<Row> dsSQLResult = new FlinkConnect().connect(env,
			// rdbmsConfig, table,
			// RDBMSConstants.NODE_TABLE);
			//
			// DataSet<Vertex> dsTableVertices = dsSQLResult.map(new
			// CreateVertices(tnPos))
			// .withBroadcastSet(dsTablesToNodes, "tables");
			//
			// if (vertices == null) {
			// vertices = dsTableVertices;
			// } else {
			// vertices = vertices.union(dsTableVertices);
			// }
			// tnPos++;
			// }
			//
			// /*
			// * convert rdbms table's tuples respectively foreign keys to edges
			// */
			// int tePos = 0;
			// for (RDBMSTable table : tablesToEdges) {
			// DataSet<Row> dsSQLResult = new FlinkConnect().connect(env,
			// rdbmsConfig, table,
			// RDBMSConstants.EDGE_TABLE);
			//
			// /*
			// * foreign keys to edges
			// */
			// if (table.getDirectionIndicator()) {
			//
			// /*
			// * iterate over all foreign keys of table
			// */
			// int fkPos = 0;
			// for (Entry<String, String> fk :
			// table.getForeignKeys().entrySet()) {
			//
			// //represents the referencing table
			// DataSet<IdKeyTuple> fkTable = vertices.filter(new
			// TableFilter(table.getTableName()))
			// .map(new VertexToIdFkTuple(fk.getKey()));
			//
			// //represents the table been referenced
			// DataSet<IdKeyTuple> pkTable = vertices.filter(new
			// TableFilter(fk.getValue()))
			// .map(new VertexToIdPkTuple());
			//
			// //join tables to get the matches of foreign key and primary key
			// values of referencing and referenced table
			// DataSet<Edge> dsFKEdges = fkTable.join(pkTable)
			// .where(1)
			// .equalTo(1)
			// .map(new Tuple2ToEdge(fk.getKey()));
			//
			// if (edges == null) {
			// edges = dsFKEdges;
			// } else {
			// edges = edges.union(dsFKEdges);
			// }
			// fkPos++;
			// }
			// }
			//
			// /*
			// * tuples to edges
			// */
			// else {
			// //represents (n:m) relation (foreign key one, foreign key two and
			// belonging properties)
			// DataSet<Tuple3<String,String,Properties>> fkPropsTable =
			// dsSQLResult.map(new FKandProps(tePos))
			// .withBroadcastSet(dsTablesToEdges, "tables");
			//
			// //set of node's gradoop id, primary key belonging to foreign key
			// one
			// DataSet<IdKeyTuple> idFkTableOne = vertices.filter(new
			// TableFilter(table.getForeignKeys().get(table.getRowHeader().getForeignKeyHeader().get(0).getName())))
			// .map(new VertexToIdPkTuple());
			//
			// //set of node's gradoop id, primary key belonging to foreign key
			// two
			// DataSet<IdKeyTuple> idFkTableTwo = vertices.filter(new
			// TableFilter(table.getForeignKeys().get(table.getRowHeader().getForeignKeyHeader().get(1).getName())))
			// .map(new VertexToIdPkTuple());
			//
			// //join keys with belonging gradoop ids to get new edges
			// DataSet<Edge> dsTupleEdges = fkPropsTable.join(idFkTableOne)
			// .where(0)
			// .equalTo(1)
			// .map(new Tuple2ToIdFkWithProps())
			// .join(idFkTableTwo)
			// .where(1)
			// .equalTo(1)
			// .map(new Tuple3ToEdge(table.getTableName()));
			//
			// if (edges == null) {
			// edges = dsTupleEdges;
			// } else {
			// edges = edges.union(dsTupleEdges);
			// }
			//
			// //create other direction edges
			// DataSet<Edge> dsTupleEdges2 = dsTupleEdges.map(new
			// EdgeToEdgeComplement());
			//
			// edges = edges.union(dsTupleEdges2);
			// }
			// tePos++;
			// }
			//
			// /*
			// * delete all primary key property and foreign key properties from
			// vertices
			// */
			// for(RDBMSTable table : tablesToNodes){
			// ArrayList<String> fkProps = new ArrayList<String>();
			// for(RowHeaderTuple rht :
			// table.getRowHeader().getForeignKeyHeader()){
			// fkProps.add(rht.getName());
			// }
			// if(finalVertices == null){
			// finalVertices = vertices.filter(new
			// TableFilter(table.getTableName())).map(new
			// DeletePKandFKs(fkProps));
			// }else{
			// finalVertices = finalVertices.union(vertices.filter(new
			// TableFilter(table.getTableName())).map(new
			// DeletePKandFKs(fkProps)));
			// }
			// }
		} catch (Exception e) {
			e.printStackTrace();
		}

//		return null;
		return new GraphDataSource(vertices,edges,config).getLogicalGraph();
		// return
		// config.getLogicalGraphFactory().fromDataSets(finalVertices,edges);

	}

	@Override
	public GraphCollection getGraphCollection() throws IOException {
		/*
		 * connection to rdbms via jdbc
		 */
		Connection con = RDBMSConnect.connect(rdbmsConfig);

		DataSet<ImportVertex<String>> vertices = null;
		DataSet<ImportEdge<String>> edges = null;

		try {

			MetaDataParser metadata = new MetaDataParser(con);
			metadata.parse();

			/*
			 * tables convert to nodes
			 */
			ArrayList<TablesToNodes> tablesToNodes = metadata.getTablesToNodes();

			/*
			 * tables convert to edges
			 */
			ArrayList<TablesToEdges> tablesToEdges = metadata.getTablesToEdges();

			System.out.println("to Nodes :\n");
			for (TablesToNodes table : tablesToNodes) {
				System.out.println(table.getTableName());
				System.out.println(table.getSqlQuery());
			}
			System.out.println("to Edges :\n");
			for (TablesToEdges table : tablesToEdges) {
				System.out.println(table.getRelationshipType());
				System.out.println(table.getSqlQuery());
			}

			/*
			 * converts rdbms table tuples to vertices
			 */
			int tablePos = 0;
			for (TablesToNodes table : tablesToNodes) {
				DataSet<Row> dsSQLResult = new FlinkConnect().connect(env, rdbmsConfig, table.getRowCount(),
						table.getSqlQuery(), table.getRowTypeInfo());
				if (tablePos == 0) {
					vertices = dsSQLResult.map(new TupleToVertex(tablePos))
							.withBroadcastSet(env.fromCollection(tablesToNodes), "tables");
				} else {
					vertices = vertices.union(dsSQLResult.map(new TupleToVertex(tablePos))
							.withBroadcastSet(env.fromCollection(tablesToNodes), "tables"));
				}
				tablePos++;
			}

			/*
			 * converts rdbms table tuples to edges
			 */
			tablePos = 0;
			for (TablesToEdges table : tablesToEdges) {
				DataSet<Row> dsSQLResult = FlinkConnect.connect(env, rdbmsConfig, table.getRowCount(),
						table.getSqlQuery(), table.getRowTypeInfo());
				if (table.isDirectionIndicator()) {
					if (tablePos == 0) {
						edges = dsSQLResult.map(new FkToEdge(tablePos))
								.withBroadcastSet(env.fromCollection(tablesToEdges), "tables");
					} else {
						edges = edges.union(dsSQLResult.map(new FkToEdge(tablePos))
								.withBroadcastSet(env.fromCollection(tablesToEdges), "tables"));
					}
				} else {
					if(tablePos == 0){
						edges = dsSQLResult.map(new TupleToEdge(tablePos)).withBroadcastSet(env.fromCollection(tablesToEdges), "tables");
					}else{
						
					}
					edges = edges.union(dsSQLResult.map(new TupleToEdge(tablePos)).withBroadcastSet(env.fromCollection(tablesToEdges), "tables"));
				}
				tablePos++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

//		return null;
		return new GraphDataSource(vertices,edges,config).getGraphCollection();
		// return
		// config.getLogicalGraphFactory().fromDataSets(finalVertices,edges);
	}
}
