package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.MapFunction;
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
import org.gradoop.flink.util.GradoopFlinkConfig;

public class CreateEdges {

	public static DataSet<Edge> create(GradoopFlinkConfig config, RdbmsConfig rdbmsConfig,
			ArrayList<TableToEdge> tablesToEdges, DataSet<Vertex> vertices) {

		DataSet<Edge> edges = null;

		int counter = 0;
		int firstCounter = 0;

		for (TableToEdge table : tablesToEdges) {
			System.out.println(counter);
			// converts foreign key relations (1:1,1:n relations)
			if (table.isDirectionIndicator()) {

				// represents vertices of referencing table
				DataSet<IdKeyTuple> fkTable = vertices.filter(new VertexLabelFilter(table.getstartTableName()))
						.map(new VertexToIdFkTuple(table.getStartAttribute().f0));

				// represents vertices referenced by current foreign key
				DataSet<IdKeyTuple> pkTable = vertices.filter(new VertexLabelFilter(table.getendTableName()))
						.map(new VertexToIdPkTuple(table.getEndAttribute().f0));

//				DataSet<Edge> dsFKEdges = fkTable.join(pkTable).where(1).equalTo(1)
//						.map(new Tuple2ToEdge(table.getEndAttribute().f0));

				if (edges == null) {
					edges = fkTable.join(pkTable).where(1).equalTo(1)
							.map(new Tuple2ToEdge(table.getEndAttribute().f0));
				} else {
					edges = edges.union(fkTable.join(pkTable).where(1).equalTo(1)
							.map(new Tuple2ToEdge(table.getEndAttribute().f0)));
				}
				firstCounter++;
			}
			// converts table tuples (n:m relations)
			else {

//				DataSet<Row> dsSQLResult = null;
//
//				try {
//					dsSQLResult = FlinkConnect.connect(config.getExecutionEnvironment(), rdbmsConfig,
//							table.getRowCount(), table.getSqlQuery(), table.getRowTypeInfo());
//
//				} catch (ClassNotFoundException e) {
//					e.printStackTrace();
//				}
//
//				// represents the two foreign key attributes and belonging
//				// properties
//				DataSet<Tuple3<String, String, Properties>> fkPropsTable = dsSQLResult.map(new FKandProps(counter))
//						.withBroadcastSet(config.getExecutionEnvironment().fromCollection(tablesToEdges), "tables");
//
//				// represents vertices in relation with foreign key one
//				DataSet<IdKeyTuple> idPkTableOne = vertices.filter(new VertexLabelFilter(table.getstartTableName()))
//						.map(new VertexToIdPkTuple(RdbmsConstants.PK_ID));
//
//				// represents vertices in relation with foreign key two
//				DataSet<IdKeyTuple> idPkTableTwo = vertices.filter(new VertexLabelFilter(table.getendTableName()))
//						.map(new VertexToIdPkTuple(RdbmsConstants.PK_ID));
//
//				DataSet<Edge> dsTupleEdges = fkPropsTable.join(idPkTableOne).where(0).equalTo(1)
//						.map(new Tuple2ToIdFkWithProps()).join(idPkTableTwo).where(1).equalTo(1)
//						.map(new Tuple3ToEdge(table.getRelationshipType()));
//
//				if (edges == null) {
//					edges = dsTupleEdges;
//				} else {
//					edges = edges.union(dsTupleEdges);
//
//				}
//
//				// creates other direction edges
//				DataSet<Edge> dsTupleEdges2 = dsTupleEdges.map(new EdgeToEdgeComplement());
//				edges.union(dsTupleEdges2);
			}
			counter++;
		}
		return edges;
	}
}
