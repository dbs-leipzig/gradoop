package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.LabelIdKeyTuple;

public class VerticesToPkTable extends RichFlatMapFunction<TableToEdge, LabelIdKeyTuple> {
	List<Vertex> vertices;

	@Override
	public void flatMap(TableToEdge table, Collector<LabelIdKeyTuple> out) throws Exception {
		String label = table.getStartAttribute().f0;
		GradoopId id;
		String key;
		
		for (Vertex v : vertices) {
			if(v.getLabel().equals(table.getendTableName())){
				id = v.getId();
				key = v.getProperties().get(table.getEndAttribute().f0).toString();
				out.collect(new LabelIdKeyTuple(label, id, key));
			}
		}
	}

	public void open(Configuration parameters) throws Exception {
		this.vertices = getRuntimeContext().getBroadcastVariable("vertices");
	}
}
