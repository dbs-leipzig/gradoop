package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.connection.FlinkConnect;
import org.gradoop.flink.io.impl.rdbms.connection.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class CreateVertices {

	public static DataSet<Vertex> create(GradoopFlinkConfig config, RDBMSConfig rdbmsConfig,
			ArrayList<TableToNode> tablesToNodes) {

		DataSet<Vertex> vertices = null;

		int tablePos = 0;

		for (TableToNode table : tablesToNodes) {

			try {
				DataSet<Row> dsSQLResult = FlinkConnect.connect(config.getExecutionEnvironment(), rdbmsConfig,
						table.getRowCount(), table.getSqlQuery(), table.getRowTypeInfo());

				if (vertices == null) {
					vertices = dsSQLResult
							.map(new RowToVertices(config.getVertexFactory(), table.getTableName(), tablePos))
							.withBroadcastSet(config.getExecutionEnvironment().fromCollection(tablesToNodes), "tables");
				} else {
					vertices = vertices.union(dsSQLResult
							.map(new RowToVertices(config.getVertexFactory(), table.getTableName(), tablePos))
							.withBroadcastSet(config.getExecutionEnvironment().fromCollection(tablesToNodes),
									"tables"));
				}

				tablePos++;

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return vertices;
	}
}
