package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
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
			ArrayList<TableToEdge> tablesToEdges, DataSet<Vertex> vertices, int downLimit, int upLimit) throws Exception {

		// DataSet<Tuple4<String,GradoopId,GradoopId,Properties>> vertexUnion =
		// vertices.map(new VertexToTuple4());

		DataSet<Edge> edges = null;

		int counter = 0;
		int innerCounter = 0;

		for (TableToEdge table : tablesToEdges) {

			// converts foreign key relations (1:1,1:n relations)
			if (table.isDirectionIndicator()) {

				// represents vertices of referencing table
				DataSet<IdKeyTuple> fkTable = vertices.filter(new VertexLabelFilter(table.getstartTableName()))
						.map(new VertexToIdFkTuple(table.getStartAttribute().f0));

				// represents vertices referenced by current foreign key
				DataSet<IdKeyTuple> pkTable = vertices.filter(new VertexLabelFilter(table.getendTableName()))
						.map(new VertexToIdPkTuple(table.getEndAttribute().f0));

				// DataSet<IdKeyTuple> tupleUnion =
				// fkTable.union(pkTable).map(new MapFunction<IdKeyTuple,
				// IdKeyTuple>() {
				// @Override
				// public IdKeyTuple map(IdKeyTuple i) throws Exception {
				// return i;
				// }
				// });

				// DataSet<Tuple3<GradoopId, GradoopId, String>> test = pkTable
				// .map(new MapFunction<IdKeyTuple, Tuple3<GradoopId, GradoopId,
				// String>>() {
				//
				// @Override
				// public Tuple3<GradoopId, GradoopId, String> map(IdKeyTuple
				// tuple) throws Exception {
				// return new Tuple3<>(GradoopId.get(), tuple.f0, tuple.f1);
				// }
				// });
				//
				// DataSet<Edge> dsFKEdges = test.groupBy(2)
				// .reduce(new ReduceFunction<Tuple3<GradoopId, GradoopId,
				// String>>() {
				//
				// @Override
				// public Tuple3<GradoopId, GradoopId, String>
				// reduce(Tuple3<GradoopId, GradoopId, String> in1,
				// Tuple3<GradoopId, GradoopId, String> in2) throws Exception {
				// return new Tuple3<>(in1.f1, in2.f1, in2.f2);
				// }
				// }).map(new CreateEdges2(table.getStartAttribute().f0));

				DataSet<Edge> dsFKEdges = fkTable.join(pkTable).where(1).equalTo(1)
						.map(new Tuple2ToEdge(table.getStartAttribute().f0));
				//
				// DataSet<Edge> test =
				// config.getExecutionEnvironment().fromElements(new Edge());
				// DataSet<Edge> test2 =
				// config.getExecutionEnvironment().fromElements(new Edge());
				//
				// DataSet<Edge> dsFKEdges = test.union(test2);

				if (innerCounter >= downLimit && innerCounter < upLimit) {
					if (edges == null) {
						edges = dsFKEdges;
					} else {
						edges = edges.union(dsFKEdges);
					}
				}
				innerCounter++;
			}
		}

		// converts table tuples (n:m relations)
		for (TableToEdge table : tablesToEdges) {
			if (!table.isDirectionIndicator()) {
				DataSet<Row> dsSQLResult = null;

				try {
					dsSQLResult = FlinkConnect.connect(config.getExecutionEnvironment(), rdbmsConfig,
							table.getRowCount(), table.getSqlQuery(), table.getRowTypeInfo());

				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

				// represents the two foreign key attributes and belonging
				// properties
				DataSet<Tuple3<String, String, Properties>> fkPropsTable = dsSQLResult.map(new FKandProps(counter))
						.withBroadcastSet(config.getExecutionEnvironment().fromCollection(tablesToEdges), "tables");

				// represents vertices in relation with foreign key one
				DataSet<IdKeyTuple> idPkTableOne = vertices.filter(new VertexLabelFilter(table.getstartTableName()))
						.map(new VertexToIdPkTuple(RdbmsConstants.PK_ID));

				// represents vertices in relation with foreign key two
				DataSet<IdKeyTuple> idPkTableTwo = vertices.filter(new VertexLabelFilter(table.getendTableName()))
						.map(new VertexToIdPkTuple(RdbmsConstants.PK_ID));

				DataSet<Edge> dsTupleEdges = fkPropsTable.joinWithHuge(idPkTableOne).where(0).equalTo(1)
						.map(new Tuple2ToIdFkWithProps()).joinWithHuge(idPkTableTwo).where(1).equalTo(1)
						.map(new Tuple3ToEdge(table.getRelationshipType()));

				if (edges == null) {
					edges = dsTupleEdges;
				} else {
					edges = edges.union(dsTupleEdges);
				}

				// creates other direction edges
				DataSet<Edge> dsTupleEdges2 = dsTupleEdges.map(new EdgeToEdgeComplement());
				edges = edges.union(dsTupleEdges2);
			}
			counter++;
		}
		return edges;
	}

}
