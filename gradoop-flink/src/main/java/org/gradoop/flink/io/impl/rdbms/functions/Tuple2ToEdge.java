package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

/**
 * Creates edges out of joined gradoop id, key name pairs
 */
public class Tuple2ToEdge implements MapFunction <Tuple2<IdKeyTuple,IdKeyTuple>, Edge>{
	
	/**
	 * Name of current foreign key attribute
	 */
	String fkName;
	
	/**
	 * Constructor
	 * @param fkName Name of current foreign key attribute
	 */
	public Tuple2ToEdge(String fkName){
		this.fkName = fkName;
	}

	@Override
	public Edge map(Tuple2<IdKeyTuple, IdKeyTuple> value) throws Exception {
		Edge e = new Edge();
		e.setId(GradoopId.get());
		e.setSourceId(value.f0.f0);
		e.setTargetId(value.f1.f0);
		e.setLabel(fkName);
		return e;
	}
}
