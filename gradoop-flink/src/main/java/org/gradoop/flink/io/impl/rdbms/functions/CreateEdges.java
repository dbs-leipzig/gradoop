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

package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.connection.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connection.RdbmsConfig;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.LabelIdKeyTuple;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Creates EPGM edges from foreign key respectively n:m relations
 *
 */
public class CreateEdges {

	/**
	 * Creates EPGM edges from foreign key respectively n:m relations
	 * @param config Gradoop flink configuration
	 * @param rdbmsConfig Relational database configuration
	 * @param tablesToEdges List of relations going to convert to edges
	 * @param vertices Dataset of already created vertices
	 * @return	Directed and undirected EPGM edges
	 * @throws Exception
	 */
	public static DataSet<Edge> create(GradoopFlinkConfig config, RdbmsConfig rdbmsConfig,
			ArrayList<TableToEdge> tablesToEdges, DataSet<Vertex> vertices) throws Exception {

		DataSet<Edge> edges = null;
	
		/*
		 *  Foreign key relations to edges
		 */
		DataSet<TableToEdge> dsTablesToEdges = config.getExecutionEnvironment().fromCollection(tablesToEdges);

		// Primary key table representation of foreign key relation
		DataSet<LabelIdKeyTuple> pkEdges = dsTablesToEdges
				.filter(new DirFilter())
				.flatMap(new VerticesToPkTable())
				.withBroadcastSet(vertices, "vertices")
				.distinct("*");

		// Foreign key table representation of foreign key relation
		DataSet<LabelIdKeyTuple> fkEdges = dsTablesToEdges
				.filter(new DirFilter())
				.flatMap(new VerticesToFkTable())
				.withBroadcastSet(vertices, "vertices");

		edges = pkEdges.join(fkEdges).where(0, 2).equalTo(0, 2).map(new JoinSetToEdges(config));

		/*
		 *  N:M relations to edges
		 */
		int counter = 0;
		for (TableToEdge table : tablesToEdges) {
			if (!table.isDirectionIndicator()) {
				
				DataSet<Row> dsSQLResult = null;

				try {
					dsSQLResult = FlinkConnect.connect(config.getExecutionEnvironment(), rdbmsConfig,
							table.getRowCount(), table.getSqlQuery(), table.getRowTypeInfo());

				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

				// Represents the two foreign key attributes and belonging
				// properties
				DataSet<Tuple3<String, String, Properties>> fkPropsTable = dsSQLResult.map(new FKandProps(counter))
						.withBroadcastSet(config.getExecutionEnvironment().fromCollection(tablesToEdges), "tables");

				// Represents vertices in relation with foreign key one
				DataSet<IdKeyTuple> idPkTableOne = vertices.filter(new VertexLabelFilter(table.getstartTableName()))
						.map(new VertexToIdPkTuple());

				// Represents vertices in relation with foreign key two
				DataSet<IdKeyTuple> idPkTableTwo = vertices.filter(new VertexLabelFilter(table.getendTableName()))
						.map(new VertexToIdPkTuple());

				DataSet<Edge> dsTupleEdges = fkPropsTable.join(idPkTableOne).where(0).equalTo(1)
						.map(new Tuple2ToIdFkWithProps()).join(idPkTableTwo).where(1).equalTo(1)
						.map(new Tuple3ToEdge(table.getRelationshipType()));

				if (edges == null) {
					edges = dsTupleEdges;
				} else {
					edges = edges.union(dsTupleEdges);
				}

				// Creates other direction edges
				edges = edges.union(dsTupleEdges.map(new EdgeToEdgeComplement()));
			}
			counter++;
		}
		
		return edges;
	}
}
