package org.gradoop.flink.io.impl.rdbms;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.rdbms.connection.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connection.RDBMSConnect;
import org.gradoop.flink.io.impl.rdbms.functions.CreateVertices;
import org.gradoop.flink.io.impl.rdbms.functions.DeletePKandFKs;
import org.gradoop.flink.io.impl.rdbms.functions.EdgeToEdgeComplement;
import org.gradoop.flink.io.impl.rdbms.functions.FKandProps;
import org.gradoop.flink.io.impl.rdbms.functions.TableFilter;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple2ToEdge;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple2ToIdFkWithProps;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple3ToEdge;
import org.gradoop.flink.io.impl.rdbms.functions.VertexToIdFkTuple;
import org.gradoop.flink.io.impl.rdbms.functions.VertexToIdPkTuple;
import org.gradoop.flink.io.impl.rdbms.metadata.MetaDataParser;
import org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Transforms a relational database into an EPGM database.
 *
 */
public class RDBMSDataSource implements DataSource {
	
	// standard gradoop flink config
	private GradoopFlinkConfig config;
	
	// config storing rdbms' url,user,pw informations
	private RDBMSConfig rdbmsConfig;
	
	// standard flink execution environment
	private ExecutionEnvironment env;

	/**
	 * Transforms a relational database with given parameters into an EPGM database
	 * @param url jdbc standard url - jdbc:[managementsystem identifier]://[host:port]/[databasename] (e.g. jdbc:mysql://localhost/employees)
	 * @param user username of database
	 * @param pw password of database
	 * @param config standard GradoopFlinkConfig
	 */
	public RDBMSDataSource(String url, String user, String pw, String jdbcDriverPath, GradoopFlinkConfig config) {
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
		
		DataSet<Vertex> tempVertices = null;
		DataSet<Vertex> vertices = null;
		DataSet<Edge> edges = null;

		try {

			MetaDataParser metadata = new MetaDataParser(con);
			metadata.parse();

			/*
			 * tables convert to nodes
			 */
			ArrayList<TableToNode> tablesToNodes = metadata.getTablesToNodes();

			/*
			 * tables convert to edges
			 */
			ArrayList<TableToEdge> tablesToEdges = metadata.getTablesToEdges();
		
			
			/************************************
			 * TEST STUFF
			 */
//			System.out.println("to Nodes :\n");
//			for (TableToNode table : tablesToNodes) {
//				System.out.println(table.getTableName());
//				System.out.println(table.getSqlQuery());
//			}
//			System.out.println("to Edges :\n");
//			for (TableToEdge table : tablesToEdges) {
//				System.out.println(table.getRefingTableName());
//				System.out.println(table.getSqlQuery());
//			}
			/*
			 *************************************
			 */
			
			
			
			/*
			 * convert rdbms table's tuples to nodes
			 */
			int tablePos = 0;
			for (TableToNode table : tablesToNodes) {
				
				//queryresult of table querying primary keys, foreign keys and attributes 
				DataSet<Row> dsSQLResult = new FlinkConnect().connect(env, rdbmsConfig, table.getRowCount(),
						table.getSqlQuery(), table.getRowTypeInfo());
			
				if(tempVertices == null){
				tempVertices = dsSQLResult.map(new CreateVertices(tablePos))
						.withBroadcastSet(env.fromCollection(tablesToNodes), "tables");
				}else{
					tempVertices = tempVertices.union(dsSQLResult.map(new CreateVertices(tablePos))
						.withBroadcastSet(env.fromCollection(tablesToNodes), "tables"));
				}
				tablePos++;
			}
			
			/*
			 * convert rdbms table's tuples respectively foreign keys to edges
			 */
			tablePos = 0;
			for (TableToEdge table : tablesToEdges) {
				/*
				 * foreign keys to edges
				 */
				if (table.isDirectionIndicator()) {
						
					//represents the referencing table
					DataSet<IdKeyTuple> fkTable = tempVertices.filter(new TableFilter(table.getstartTableName()))
							.map(new VertexToIdFkTuple(table.getEndAttribute().f0));
					
					//represents the table been referenced
					DataSet<IdKeyTuple> pkTable = tempVertices.filter(new TableFilter(table.getendTableName()))
							.map(new VertexToIdPkTuple());
					
					//join tables to get the matches of foreign key and primary key values of referencing and referenced table
					DataSet<Edge> dsFKEdges = fkTable.join(pkTable)
							.where(1)
							.equalTo(1)
							.map(new Tuple2ToEdge(table.getEndAttribute().f0));
					
					if (edges == null) {
						edges = dsFKEdges;
					} else {
						edges = edges.union(dsFKEdges);
					}
				}

				/*
				 * tuples to edges
				 */
				else {
					DataSet<Row> dsSQLResult = FlinkConnect.connect(env, rdbmsConfig, table.getRowCount(), table.getSqlQuery(), table.getRowTypeInfo());
					//represents (n:m) relation (foreign key one, foreign key two and belonging properties)
					DataSet<Tuple3<String,String,Properties>> fkPropsTable = dsSQLResult.map(new FKandProps(tablePos))
							.withBroadcastSet(env.fromCollection(tablesToEdges),"tables");
									
					//set of node's gradoop id, primary key belonging to foreign key one
					DataSet<IdKeyTuple> idFkTableOne = tempVertices.filter(new TableFilter(table.getstartTableName()))
							.map(new VertexToIdPkTuple());

					//set of node's gradoop id, primary key belonging to foreign key two 
					DataSet<IdKeyTuple> idFkTableTwo = tempVertices.filter(new TableFilter(table.getendTableName()))
							.map(new VertexToIdPkTuple());
					
					//join keys with belonging gradoop ids to get new edges
					DataSet<Edge> dsTupleEdges = fkPropsTable.join(idFkTableOne)
							.where(0)
							.equalTo(1)
							.map(new Tuple2ToIdFkWithProps())
							.join(idFkTableTwo)
							.where(1)
							.equalTo(1)
							.map(new Tuple3ToEdge(table.getRelationshipType()));
										
					if (edges == null) {
						edges = dsTupleEdges;
					} else {
						edges = edges.union(dsTupleEdges);
					}
					
					//create other direction edges
					DataSet<Edge> dsTupleEdges2 = dsTupleEdges.map(new EdgeToEdgeComplement());
					
					edges = edges.union(dsTupleEdges2);
				}
				tablePos++;
			}
						
			/*
			 * delete all primary key property and foreign key properties from vertices 
			 */
			for(TableToNode table : tablesToNodes){
				ArrayList<String> fkProps = new ArrayList<String>();
				for(RowHeaderTuple rht : table.getRowheader().getForeignKeyHeader()){
					fkProps.add(rht.getName());
				}
				if(vertices == null){
					vertices = tempVertices.filter(new TableFilter(table.getTableName())).map(new DeletePKandFKs(fkProps));
				}else{
					vertices = vertices.union(tempVertices.filter(new TableFilter(table.getTableName())).map(new DeletePKandFKs(fkProps)));
				}
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
