package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
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
		String key = "";
		try{
			key = v.getProperties().get(fkName).toString();
		}catch(Exception e){
			System.out.println("Foreign Key " + fkName + " not found.");
		}
		return new IdKeyTuple(v.getId(),key);
	}
}
