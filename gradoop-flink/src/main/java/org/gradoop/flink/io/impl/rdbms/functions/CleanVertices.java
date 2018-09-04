package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;

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
	public static DataSet<Vertex> clean(ArrayList<TableToNode> tablesToNodes, DataSet<Vertex> tempVertices){
		DataSet<Vertex> vertices = null;
		
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
		return vertices;
	}
}
