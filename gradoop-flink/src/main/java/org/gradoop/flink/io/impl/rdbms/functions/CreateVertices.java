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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.types.Row;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.connection.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connection.RdbmsConfig;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class CreateVertices {

	public static DataSet<Vertex> create(GradoopFlinkConfig config, RdbmsConfig rdbmsConfig,
			ArrayList<TableToNode> tablesToNodes) {
		DataSet<Vertex> vertices = null;
		EPGMVertexFactory vertexFactory = config.getVertexFactory();

		int counter = 0;

		for (TableToNode table : tablesToNodes) {
			if(counter < 52) {
			
				try {
					DataSet<Row> dsSQLResult = FlinkConnect.connect(config.getExecutionEnvironment(), rdbmsConfig,
							table.getRowCount(), table.getSqlQuery(), table.getRowTypeInfo());

					if (vertices == null) {
						vertices = dsSQLResult.map(new RowToVertices(vertexFactory, table.getTableName(), counter))
								.withBroadcastSet(config.getExecutionEnvironment().fromCollection(tablesToNodes),
										"tables");
					} else {
						vertices = vertices.union(dsSQLResult
								.map(new RowToVertices(vertexFactory, table.getTableName(), counter)).withBroadcastSet(
										config.getExecutionEnvironment().fromCollection(tablesToNodes), "tables"));
					}

				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
			counter++;
		}
		return vertices;
	}
}
