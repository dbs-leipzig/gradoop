package org.gradoop.flink.io.impl.rdbms;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.rdbms.connect.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConnect;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.CreateVertices;
import org.gradoop.flink.io.impl.rdbms.functions.DeleteFKs;
import org.gradoop.flink.io.impl.rdbms.functions.FKandProps;
import org.gradoop.flink.io.impl.rdbms.functions.TableFilter;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple2ToEdge;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple2ToIdFkWithProps;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple3ToEdge;
import org.gradoop.flink.io.impl.rdbms.functions.VertexToIdFkTuple;
import org.gradoop.flink.io.impl.rdbms.functions.VertexToIdPkTuple;
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
		
		DataSet<Vertex> vertices = null;
		DataSet<Edge> edges = null;
		try {

			/*
			 * set of rdbms' metadata
			 */
			ArrayList<RDBMSTable> tables = SequentialMetaDataParser.parse(RDBMSMetadata.getDBMetaData(con), con);
			System.out.println("***Metadata of " + tables.size() + " tables determined");
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

				DataSet<Vertex> dsTableVertices = dsSQLResult.map(new CreateVertices(tnPos))
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
						
						DataSet<Tuple2<GradoopId,String>> fkTable = vertices.filter(new TableFilter(table.getTableName()))
								.map(new VertexToIdFkTuple(fk.getKey()));
						
						DataSet<Tuple2<GradoopId,String>> pkTable = vertices.filter(new TableFilter(fk.getValue()))
								.map(new VertexToIdPkTuple());
						
						
						DataSet<Edge> dsFKEdges = fkTable.join(pkTable)
								.where(1)
								.equalTo(1)
								.map(new Tuple2ToEdge(fk.getKey()));
						
						if (edges == null) {
							edges = dsFKEdges;
						} else {
							edges = edges.union(dsFKEdges);
						}
						fkPos++;
					}
				}

				/*
				 * tuples to edges
				 */

				else {
					
					DataSet<Tuple3<String,String,Properties>> fkPropsTable = dsSQLResult.map(new FKandProps(tePos))
							.withBroadcastSet(dsTablesToEdges, "tables");
										
					DataSet<Tuple2<GradoopId,String>> idFkTableOne = vertices.filter(new TableFilter(table.getForeignKeys().get(table.getRowHeader().getForeignKeyHeader().get(0).getName())))
							.map(new VertexToIdPkTuple());

					DataSet<Tuple2<GradoopId,String>> idFkTableTwo = vertices.filter(new TableFilter(table.getForeignKeys().get(table.getRowHeader().getForeignKeyHeader().get(1).getName())))
							.map(new VertexToIdPkTuple());
					
					DataSet<Edge> dsFKEdges = fkPropsTable.join(idFkTableOne)
							.where(0)
							.equalTo(1)
							.map(new Tuple2ToIdFkWithProps())
							.join(idFkTableTwo)
							.where(1)
							.equalTo(1)
							.map(new Tuple3ToEdge(table.getTableName()));
										
					if (edges == null) {
						edges = dsFKEdges;
					} else {
						edges = edges.union(dsFKEdges);
					}
				}
				tePos++;
			}
			
			for(RDBMSTable table : tablesToNodes){
				ArrayList<String> fkProps = new ArrayList<String>();
				for(RowHeaderTuple rht : table.getRowHeader().getForeignKeyHeader()){
					fkProps.add(rht.getName());
				}
				
				vertices = vertices.map(new DeleteFKs(fkProps));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
//		return null;
		return config.getLogicalGraphFactory().fromDataSets(vertices,edges);		
	}

	@Override
	public GraphCollection getGraphCollection() throws IOException {
		return config.getGraphCollectionFactory().fromGraph(getLogicalGraph());
	}
}
