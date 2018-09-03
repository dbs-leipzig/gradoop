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

		DataSet<Edge> edges = null;
		DataSet<Edge> directedEdges = null;
		DataSet<Edge> undirectedEdges = null;

		/* 
		 * alternative implementation to avoid flink`s max 64 outputs exception (just for directed edges)
		 */
		//**************************************************************************
		
		 DataSet<TableToEdge> dsTablesToEdges =
		 config.getExecutionEnvironment().fromCollection(tablesToEdges);
		
		 // converts directed edges
		
		 DataSet<LabelIdKeyTuple> pkEdges = dsTablesToEdges.filter(new
		 DirFilter())
		 .flatMap(new VerticesToPkTable()).withBroadcastSet(vertices,
		 "vertices").distinct(0,1);
		
		 DataSet<LabelIdKeyTuple> fkEdges = dsTablesToEdges.filter(new
		 DirFilter())
		 .flatMap(new VerticesToFkTable()).withBroadcastSet(vertices,
		 "vertices").distinct(0,1);
		
		 directedEdges = pkEdges.join(fkEdges).where(0, 2).equalTo(0, 2).map(new
		 JoinSetToEdges(config));
		
		//****************************************************************************

		// converts undirected edges
		int counter = 0;

		for (TableToEdge table : tablesToEdges) {
			if (table.isDirectionIndicator()) {
//				DataSet<IdKeyTuple> pkTables = vertices.filter(new VertexLabelFilter(table.getendTableName()))
//						.map(new VertexToIdPkTuple(table.getEndAttribute().f0));
//
//				DataSet<IdKeyTuple> fkTables = vertices.filter(new VertexLabelFilter(table.getstartTableName()))
//						.map(new VertexToIdFkTuple(table.getStartAttribute().f0));
//
//				edges = edges.union(
//						pkTables.join(fkTables).where(1).equalTo(1).map(new Tuple2ToEdge(table.getEndAttribute().f0)));
			} else {
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

				if(undirectedEdges == null) {
					undirectedEdges = dsTupleEdges;
				}else {
					undirectedEdges = undirectedEdges.union(dsTupleEdges);
				}
				
				// creates other direction edges
				undirectedEdges = undirectedEdges.union(dsTupleEdges.map(new EdgeToEdgeComplement()));
			}
			counter++;
		}
		
		if(directedEdges == null) {
			if(undirectedEdges == null) {
				edges = null;
			}else {
				edges = undirectedEdges;
			}
		}else if(undirectedEdges == null) {
			edges = directedEdges;
		}else if(undirectedEdges != null) {
			edges = directedEdges.union(undirectedEdges);
		}
		return edges;
	}
}
