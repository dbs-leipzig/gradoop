package org.gradoop.flink.io.impl.rdbms;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
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
import org.gradoop.flink.io.impl.rdbms.functions.EdgeToEdgeComplement;
import org.gradoop.flink.io.impl.rdbms.functions.FKandProps;
import org.gradoop.flink.io.impl.rdbms.functions.SequentialMetaDataParser;
import org.gradoop.flink.io.impl.rdbms.functions.TableFilter;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple2ToEdge;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple2ToIdFkWithProps;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple3ToEdge;
import org.gradoop.flink.io.impl.rdbms.functions.VertexToIdFkTuple;
import org.gradoop.flink.io.impl.rdbms.functions.VertexToIdPkTuple;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSMetadata;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
/**
 * Transforms a relational database into an EPGM database.
 *
 */
public class RDBMSDataSource implements DataSource {
	private GradoopFlinkConfig config;
	private RDBMSConfig rdbmsConfig;
	private ExecutionEnvironment env;

	/**
	 * 
	 * @param url jdbc standard url - jdbc:[managementsystem name]://[host:port]/[databasename] (e.g. jdbc:mysql://localhost/employees)
	 * @param user username of database
	 * @param pw password of database
	 * @param config standard GradoopFlinkConfig
	 */
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
		DataSet<Vertex> finalVertices = null;
		DataSet<Edge> edges = null;
		
		try {

			/*
			 * list of rdbms tables storing conversion important metadata of every database table
			 */
			ArrayList<RDBMSTable> tables = SequentialMetaDataParser.parse(con.getMetaData(), con);
		
			/*
			 * tables convert to nodes
			 */
			ArrayList<RDBMSTable> tablesToNodes = new ArrayList<RDBMSTable>();
			
			/*
			 * tables convert to edges
			 */
			ArrayList<RDBMSTable> tablesToEdges = new ArrayList<RDBMSTable>();
			
			/*
			 * assign tables with help of direction indicator 
			 */
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
				
				//queryresult of table querying primary keys, foreign keys and attributes 
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
						
						//represents the referencing table
						DataSet<IdKeyTuple> fkTable = vertices.filter(new TableFilter(table.getTableName()))
								.map(new VertexToIdFkTuple(fk.getKey()));
						
						//represents the table been referenced
						DataSet<IdKeyTuple> pkTable = vertices.filter(new TableFilter(fk.getValue()))
								.map(new VertexToIdPkTuple());
						
						//join tables to get the matches of foreign key and primary key values 
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
					
					//represents (n:m) relation (foreign key one, foreign key two and belonging properties)
					DataSet<Tuple3<String,String,Properties>> fkPropsTable = dsSQLResult.map(new FKandProps(tePos))
							.withBroadcastSet(dsTablesToEdges, "tables");
									
					//set of node's gradoop id, primary key belonging to foreign key one
					DataSet<IdKeyTuple> idFkTableOne = vertices.filter(new TableFilter(table.getForeignKeys().get(table.getRowHeader().getForeignKeyHeader().get(0).getName())))
							.map(new VertexToIdPkTuple());

					//set of node's gradoop id, primary key belonging to foreign key two 
					DataSet<IdKeyTuple> idFkTableTwo = vertices.filter(new TableFilter(table.getForeignKeys().get(table.getRowHeader().getForeignKeyHeader().get(1).getName())))
							.map(new VertexToIdPkTuple());
					
					//join keys with belonging gradoop ids to get new edges
					DataSet<Edge> dsTupleEdges = fkPropsTable.join(idFkTableOne)
							.where(0)
							.equalTo(1)
							.map(new Tuple2ToIdFkWithProps())
							.join(idFkTableTwo)
							.where(1)
							.equalTo(1)
							.map(new Tuple3ToEdge(table.getTableName()));
										
					if (edges == null) {
						edges = dsTupleEdges;
					} else {
						edges = edges.union(dsTupleEdges);
					}
					
					//create other direction edges
					DataSet<Edge> dsTupleEdges2 = dsTupleEdges.map(new EdgeToEdgeComplement());
					
					edges = edges.union(dsTupleEdges2);
				}
				tePos++;
			}
			
			/*
			 * delete all foreign key properties from vertices 
			 */
			for(RDBMSTable table : tablesToNodes){
				ArrayList<String> fkProps = new ArrayList<String>();
				for(RowHeaderTuple rht : table.getRowHeader().getForeignKeyHeader()){
					fkProps.add(rht.getName());
				}
				if(finalVertices == null){
					finalVertices = vertices.filter(new TableFilter(table.getTableName())).map(new DeleteFKs(fkProps));
				}else{
					finalVertices = finalVertices.union(vertices.filter(new TableFilter(table.getTableName())).map(new DeleteFKs(fkProps)));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return config.getLogicalGraphFactory().fromDataSets(finalVertices,edges);		
	}

	@Override
	public GraphCollection getGraphCollection() throws IOException {
		return config.getGraphCollectionFactory().fromGraph(getLogicalGraph());
	}
}
