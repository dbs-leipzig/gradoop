package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

/**
 * Creates pairs consisting of gradoop id and foreign key name, from vertices
 */
public class VertexToIdFkTuple implements MapFunction<Vertex,IdKeyTuple> {
	
	/**
	 * Name of current foreign key attribute
	 */
	private String fkName;
	
	/**
	 * Constructor
	 * @param fkName Name of current foreign key attribute
	 */
	public VertexToIdFkTuple(String fkName){
		this.fkName = fkName;
	}
	
	@Override
	public IdKeyTuple map(Vertex v) throws Exception {
		return new IdKeyTuple(v.getId(),v.getProperties().get(fkName).toString());
	}
}
