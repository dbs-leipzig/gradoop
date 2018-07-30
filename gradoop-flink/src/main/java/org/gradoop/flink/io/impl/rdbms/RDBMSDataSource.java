/**
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
package org.gradoop.flink.io.impl.rdbms;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.rdbms.connection.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connection.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.connection.RDBMSConnect;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.CreateVertices;
import org.gradoop.flink.io.impl.rdbms.functions.DeletePKandFKs;
import org.gradoop.flink.io.impl.rdbms.functions.EdgeToEdgeComplement;
import org.gradoop.flink.io.impl.rdbms.functions.FKandProps;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple2ToEdge;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple2ToIdFkWithProps;
import org.gradoop.flink.io.impl.rdbms.functions.Tuple3ToEdge;
import org.gradoop.flink.io.impl.rdbms.functions.VertexLabelFilter;
import org.gradoop.flink.io.impl.rdbms.functions.VertexToIdFkTuple;
import org.gradoop.flink.io.impl.rdbms.functions.VertexToIdPkTuple;
import org.gradoop.flink.io.impl.rdbms.metadata.MetaDataParser;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A graph data import for relational databases.
 */
public class RDBMSDataSource implements DataSource {

	/**
	 * Gradoop Flink configuration
	 */
	private GradoopFlinkConfig config;

	/**
	 * Configuration for the relational database connection
	 */
	private RDBMSConfig rdbmsConfig;

	/**
	 * Flink Execution Environment
	 */
	private ExecutionEnvironment env;

	/**
	 * Temporary vertices
	 */
	private DataSet<Vertex> tempVertices;

	/**
	 * Vertices
	 */
	private DataSet<Vertex> vertices;

	/**
	 * Edges
	 */
	private DataSet<Edge> edges;
	
	/**
	 * Transforms a relational database into an EPGM instance
	 * 
	 * The datasource expects a standard jdbc url, e.g.
	 * (jdbc:mysql://localhost/employees) and a valid path to a fitting jdbc
	 * driver
	 * 
	 * @param url
	 *            Valid jdbc url (e.g. jdbc:mysql://localhost/employees)
	 * @param user
	 *            Username of relational database user
	 * @param pw
	 *            Password of relational database user
	 * @param jdbcDriverPath
	 *            Valid path to jdbc driver
	 * @param jdbcDriverClassName
	 *            Valid jdbc driver class name
	 * @param config
	 *            Gradoop Flink configuration
	 */
	public RDBMSDataSource(String url, String user, String pw, String jdbcDriverPath, String jdbcDriverClassName,
			GradoopFlinkConfig config) {
		this.config = config;
		this.rdbmsConfig = new RDBMSConfig(url, user, pw, jdbcDriverPath, jdbcDriverClassName);
		this.env = config.getExecutionEnvironment();
	}

	@Override
	public LogicalGraph getLogicalGraph() {
		Connection con = RDBMSConnect.connect(rdbmsConfig);

		try {
			// creates a metadata representation of the connected relational
			// database schema
			MetaDataParser metadata = new MetaDataParser(con);
			metadata.parse();

			// tables going to convert to vertices
			ArrayList<TableToNode> tablesToNodes = metadata.getTablesToNodes();

			// tables going to convert to edges
			ArrayList<TableToEdge> tablesToEdges = metadata.getTablesToEdges();

			// ********************
			System.out.println("\n****************************");
			System.out.println("Tables to Nodes");
			System.out.println("\n****************************");
			for (TableToNode table : tablesToNodes) {
				System.out.println("\n" + table.getTableName());
				System.out.println("*************");
				for (NameTypeTuple pk : table.getPrimaryKeys()) {
					System.out.println(pk.f0 + " " + pk.f1 + " pk");
				}
				for (FkTuple fk : table.getForeignKeys()) {
					System.out.println(fk.f0 + " " + fk.f1 + " " + fk.f2 + " " + fk.f3 + " fk");
				}
				for (NameTypeTypeTuple att : table.getFurtherAttributes()) {
					System.out.println(att.f0 + " " + att.f1 + " " + att.f2 + " att");
				}
			}
			System.out.println("\n****************************");
			System.out.println("Tables to Edges");
			System.out.println("\n****************************");
			for (TableToEdge table : tablesToEdges) {
				System.out.println("\n" + table.getRelationshipType());
				System.out.println("*************");
				System.out.println(table.getstartTableName() + " " + table.getendTableName() + " "
						+ table.getStartAttribute() + " " + table.getEndAttribute());
				try {
					for (NameTypeTuple pk : table.getPrimaryKeys()) {
						System.out.println(pk.f0 + " " + pk.f1 + " pk");
					}
					for (NameTypeTypeTuple att : table.getFurtherAttributes()) {
						System.out.println(att.f0 + " " + att.f1 + " " + att.f2 + " att");
					}
				} catch (Exception e) {
				}
			}
			// ********************

			// creates vertices from rdbms table tuples
			int tablePos = 0;
			for (TableToNode table : tablesToNodes) {
				DataSet<Row> dsSQLResult = FlinkConnect.connect(env, rdbmsConfig, table.getRowCount(),
						table.getSqlQuery(), table.getRowTypeInfo());

				if (tempVertices == null) {
					tempVertices = dsSQLResult.map(new CreateVertices(table.getTableName(), tablePos))
							.withBroadcastSet(env.fromCollection(tablesToNodes), "tables");
				} else {
					tempVertices = tempVertices
							.union(dsSQLResult.map(new CreateVertices(table.getTableName(), tablePos))
									.withBroadcastSet(env.fromCollection(tablesToNodes), "tables"));
				}
				tablePos++;
			}

			// creates edges from rdbms table tuples and foreign key relations
			tablePos = 0;
			for (TableToEdge table : tablesToEdges) {

				// converts foreign key relations (1:1,1:n relations)
				if (table.isDirectionIndicator()) {

					// represents vertices of referencing table
					DataSet<IdKeyTuple> fkTable = tempVertices.filter(new VertexLabelFilter(table.getstartTableName()))
							.map(new VertexToIdFkTuple(table.getStartAttribute().f0));

					// represents vertices referenced by current foreign key
					DataSet<IdKeyTuple> pkTable = tempVertices.filter(new VertexLabelFilter(table.getendTableName()))
							.map(new VertexToIdPkTuple(table.getEndAttribute().f0));

					DataSet<Edge> dsFKEdges = fkTable.join(pkTable).where(1).equalTo(1)
							.map(new Tuple2ToEdge(table.getEndAttribute().f0));

					if(edges == null) {
						edges = dsFKEdges;
					}else {
						edges = edges.union(dsFKEdges);
					}
				}

				// converts table tuples (n:m relations)
				else {
					DataSet<Row> dsSQLResult = FlinkConnect.connect(env, rdbmsConfig, table.getRowCount(),
							table.getSqlQuery(), table.getRowTypeInfo());

					// represents the two foreign key attributes and belonging
					// properties
					DataSet<Tuple3<String, String, Properties>> fkPropsTable = dsSQLResult.map(new FKandProps(tablePos))
							.withBroadcastSet(env.fromCollection(tablesToEdges), "tables");

					// represents vertices in relation with foreign key one
					DataSet<IdKeyTuple> idPkTableOne = tempVertices
							.filter(new VertexLabelFilter(table.getstartTableName())).map(new VertexToIdPkTuple(RDBMSConstants.PK_ID));

					// represents vertices in relation with foreign key two
					DataSet<IdKeyTuple> idPkTableTwo = tempVertices
							.filter(new VertexLabelFilter(table.getendTableName())).map(new VertexToIdPkTuple(RDBMSConstants.PK_ID));

					DataSet<Edge> dsTupleEdges = fkPropsTable.join(idPkTableOne).where(0).equalTo(1)
							.map(new Tuple2ToIdFkWithProps()).join(idPkTableTwo).where(1).equalTo(1)
							.map(new Tuple3ToEdge(table.getRelationshipType()));

					if (edges == null) {
						edges = dsTupleEdges;
					} 
					else {
						edges = edges.union(dsTupleEdges);
					}

					// creates other direction edges
					DataSet<Edge> dsTupleEdges2 = dsTupleEdges.map(new EdgeToEdgeComplement());

					edges = edges.union(dsTupleEdges2);
				}
				tablePos++;
			}
					
			// cleans vertices by deleting primary key and foreign key
			// properties
			for (TableToNode table : tablesToNodes) {

				// used to find foreign key properties
				ArrayList<String> fkProps = new ArrayList<String>();

				for (FkTuple fk : table.getForeignKeys()) {
					fkProps.add(fk.f0);
				}

				if (vertices == null) {
					vertices = tempVertices.filter(new VertexLabelFilter(table.getTableName()))
							.map(new DeletePKandFKs(fkProps));
				} else {
					vertices = vertices.union(tempVertices.filter(new VertexLabelFilter(table.getTableName()))
							.map(new DeletePKandFKs(fkProps)));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
//		return null;
		return config.getLogicalGraphFactory().fromDataSets(vertices, edges);
	}

	@Override
	public GraphCollection getGraphCollection() throws IOException {
		return config.getGraphCollectionFactory().fromGraph(getLogicalGraph());
	}
}
