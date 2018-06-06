package org.gradoop.flink.io.impl.rdbms;

import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.rdbms.connect.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConnect;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.CreateEdges;
import org.gradoop.flink.io.impl.rdbms.functions.CreateEdgesWithProps;
import org.gradoop.flink.io.impl.rdbms.functions.NodeFilter;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSMetadata;
import org.gradoop.flink.io.impl.rdbms.sequential.SequentialMetaDataParser;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class RDBMSDataSource implements DataSource {
	private GradoopFlinkConfig config;
	private RDBMSConfig rdbmsConfig;
	private ExecutionEnvironment env;

	public RDBMSDataSource(String url, String user, String pw, GradoopFlinkConfig config) {
		this.config = config;
		this.rdbmsConfig = new RDBMSConfig(url, user, pw);
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

			/*
			 * set of rdbms' metadata
			 */
			ArrayList<RDBMSTable> tables = SequentialMetaDataParser.parse(RDBMSMetadata.getDBMetaData(con), con);

			/*
			 * create two sets of tables - tablesToNodes: tuples of this tables
			 * are going to convert to nodes of the epgm - tablesToEdges: tuples
			 * of this tables are going to convert to edges of the epgm
			 */
			ArrayList<RDBMSTable> tablesToNodes = new ArrayList<RDBMSTable>();
			ArrayList<RDBMSTable> tablesToEdges = new ArrayList<RDBMSTable>();
			for (RDBMSTable table : tables) {
				if (!table.getDirectionIndicator()) {
					tablesToEdges.add(table);
				} else {
					tablesToNodes.add(table);
					if (!table.getForeignKeys().isEmpty()) {
						RDBMSTable tableCopy = table.clone();
						tablesToEdges.add(tableCopy);
					}
				}
			}

			/*
			 * dataset representation of the created sets
			 */
			DataSet<RDBMSTable> dsTablesToNodes = env.fromCollection(tablesToNodes);
			DataSet<RDBMSTable> dsTablesToEdges = env.fromCollection(tablesToEdges);

			/*
			 * convert rdbms table's tuples to nodes
			 */
			int tnPos = 0;
			for (RDBMSTable table : tablesToNodes) {
				DataSet<Row> dsSQLResult = new FlinkConnect().connect(env, rdbmsConfig, table,
						RDBMSConstants.NODE_TABLE);

				DataSet<ImportVertex<String>> dsTableVertices = dsSQLResult.map(new CreateVertices(tnPos))
						.withBroadcastSet(dsTablesToNodes, "tables");
					
				if (vertices == null) {
					vertices = dsTableVertices;
				} else {
					vertices = vertices.union(dsTableVertices);
				}
				tnPos++;
			}
			/*
			 * convert rdbms table's tuples respectively foreign keys to edges
			 */
			int tePos = 0;
			for (RDBMSTable table : tablesToEdges) {
				DataSet<Row> dsSQLResult = new FlinkConnect().connect(env, rdbmsConfig, table,
						RDBMSConstants.EDGE_TABLE);
				/*
				 * foreign keys to edges
				 */
				if (table.getDirectionIndicator()) {
					/*
					 * iterate over all foreign keys of table
					 */
					int fkPos = 0;
					for (Entry<String, String> fk : table.getForeignKeys().entrySet()) {

						DataSet<ImportEdge<String>> dsTableEdges = dsSQLResult.map(new CreateEdges(tePos, fkPos))
								.withBroadcastSet(dsTablesToEdges, "tables")
								.withBroadcastSet(vertices, "vertices");
						if (edges == null) {
							edges = dsTableEdges;
						} else {
							edges = edges.union(dsTableEdges);
						}
						fkPos++;
					}
				}

				/*
				 * tuples to edges
				 */

				else {
					DataSet<ImportEdge<String>> dsTableEdges = dsSQLResult.map(new CreateEdgesWithProps(tePos))
							.withBroadcastSet(dsTablesToEdges, "tables");

					if (edges == null) {
						edges = dsTableEdges;
					} else {
						edges = edges.union(dsTableEdges);
					}
				}
				tePos++;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new GraphDataSource(vertices,edges,config).getLogicalGraph();		
//		return null;
	}

	@Override
	public GraphCollection getGraphCollection() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
