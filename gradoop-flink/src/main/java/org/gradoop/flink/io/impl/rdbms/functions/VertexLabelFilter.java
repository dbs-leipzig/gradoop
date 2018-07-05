package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Filters a set of vertices by label
 */
public class VertexLabelFilter implements FilterFunction<Vertex> {
	/**
	 * Label to search for
	 */
	String label;
	
	/**
	 * Constructor
	 * @param label Label to search for
	 */
	public VertexLabelFilter(String label){
		this.label = label;
	}
	
	@Override
	public boolean filter(Vertex v) throws Exception {
		return v.getLabel().equals(label);
	}

}
