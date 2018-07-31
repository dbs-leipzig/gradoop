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
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.rdbms.connection.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connection.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.connection.RDBMSConnect;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.CleanVertices;
import org.gradoop.flink.io.impl.rdbms.functions.CreateEdges;
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

import com.sun.tools.internal.xjc.model.SymbolSpace;

/**
 * A graph data import for relational databases.
 */
public class RdbmsDataSource implements DataSource {

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
	 * (jdbc:mysql://localhost/employees) and a valid path to a fitting jdbc driver
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
	public RdbmsDataSource(String url, String user, String pw, String jdbcDriverPath, String jdbcDriverClassName,
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

			// creates vertices from rdbms table tuples
			tempVertices = CreateVertices.create(config, rdbmsConfig, tablesToNodes);

			// creates edges from rdbms table tuples and foreign key relations
			edges = CreateEdges.create(config, rdbmsConfig, tablesToEdges, tempVertices);

			// cleans vertices by deleting primary key and foreign key
			// properties
			vertices = CleanVertices.clean(tablesToNodes, tempVertices);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return config.getLogicalGraphFactory().fromDataSets(vertices, edges);
	}

	@Override
	public GraphCollection getGraphCollection() throws IOException {
		return config.getGraphCollectionFactory().fromGraph(getLogicalGraph());
	}
}
