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
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Deletes foreign key respectively primary key attributes from vertex' properties
 *
 */
public class CleanVertices {

	/**
	 * 
	 * @param tablesToNodes List of 
	 * @param tempVertices Uncleaned vertices
	 * @return
	 */
	public static DataSet<Vertex> clean(GradoopFlinkConfig config, ArrayList<TableToNode> tablesToNodes, DataSet<Vertex> tempVertices){
		DataSet<TableToNode> dsTablesToNodes = config.getExecutionEnvironment().fromCollection(tablesToNodes);
		return dsTablesToNodes.flatMap(new CleanMapper(config.getVertexFactory())).withBroadcastSet(tempVertices, "vertices");
		
		
//		DataSet<Vertex> vertices = null;
//		EPGMVertexFactory vertexFactory = config.getVertexFactory();
//
//		for (TableToNode table : tablesToNodes) {
//
//			// used to find foreign key properties
//			ArrayList<String> fkProps = new ArrayList<String>();
//
//			for (FkTuple fk : table.getForeignKeys()) {
//				fkProps.add(fk.f0);
//			}
//
//			if (vertices == null) {
//				vertices = tempVertices.filter(new VertexLabelFilter(table.getTableName()))
//						.map(new DeletePKandFKs(vertexFactory,fkProps));
//			} else {
//				vertices = vertices.union(tempVertices.filter(new VertexLabelFilter(table.getTableName()))
//						.map(new DeletePKandFKs(vertexFactory,fkProps)));
//			}
//		}
//		return vertices;
	}
}
