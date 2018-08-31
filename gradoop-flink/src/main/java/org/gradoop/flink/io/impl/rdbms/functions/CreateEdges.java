package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.gradoop.flink.io.impl.rdbms.tuples.LabelIdKeyTuple;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class CreateEdges {

	public static DataSet<Edge> create(GradoopFlinkConfig config, RdbmsConfig rdbmsConfig,
			ArrayList<TableToEdge> tablesToEdges, DataSet<Vertex> vertices) throws Exception {
		
		IterativeDataSet<TableToEdge> dsTablesToEdges = config.getExecutionEnvironment().fromCollection(tablesToEdges).iterate(tablesToEdges.size());
		
		DataSet<String> iterationEdges = dsTablesToEdges.map(new MapFunction<TableToEdge, String>() {
			@Override
			public String map(TableToEdge table) throws Exception {
				return table.getendTableName();
			}
		});
		
		DataSet<String> result = dsTablesToEdges.closeWith(iterationEdges);
		
		
		

		DataSet<Edge> edges = config.getExecutionEnvironment().fromElements(new Edge());
		DataSet<Edge> innerEdges = config.getExecutionEnvironment().fromElements(new Edge());
		DataSet<Edge> edges1;
		// DataSet<TableToEdge> dsTablesToEdges =
		// config.getExecutionEnvironment().fromCollection(tablesToEdges);
		//
		// // converts directed edges
		//
		// DataSet<LabelIdKeyTuple> pkEdges = dsTablesToEdges.filter(new
		// DirFilter())
		// .flatMap(new VerticesToPkTable()).withBroadcastSet(vertices,
		// "vertices");
		//
		// DataSet<LabelIdKeyTuple> fkEdges = dsTablesToEdges.filter(new
		// DirFilter())
		// .flatMap(new VerticesToFkTable()).withBroadcastSet(vertices,
		// "vertices");
		//
		// edges = pkEdges.join(fkEdges).where(0, 2).equalTo(0, 2).map(new
		// JoinSetToEdges(config));

		// converts undirected edges
		int counter = 0;
		int innerCounter = 0;
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

				edges = edges.union(dsTupleEdges);

				// creates other direction edges
				DataSet<Edge> dsTupleEdges2 = dsTupleEdges.map(new EdgeToEdgeComplement());
				edges = edges.union(dsTupleEdges2);
			} else {
				
				
				DataSet<IdKeyTuple> pkTables = vertices.filter(new VertexLabelFilter(table.getendTableName()))
						.map(new VertexToIdPkTuple(table.getEndAttribute().f0));

				DataSet<IdKeyTuple> fkTables = vertices.filter(new VertexLabelFilter(table.getstartTableName()))
						.map(new VertexToIdFkTuple(table.getStartAttribute().f0));

				innerEdges = innerEdges.union(
						pkTables.join(fkTables).where(1).equalTo(1).map(new Tuple2ToEdge(table.getEndAttribute().f0)));
				
				if(innerCounter%10==0){
					edges1 = edges.map(new MapFunction<Edge, Edge>() {
						private static final long serialVersionUID = 1L;
						@Override
						public Edge map(Edge e) throws Exception {
							return e;
						}
					});
				}
				
				innerCounter++;
			}
			counter++;
		}
		return edges.union(innerEdges);
	}
}
