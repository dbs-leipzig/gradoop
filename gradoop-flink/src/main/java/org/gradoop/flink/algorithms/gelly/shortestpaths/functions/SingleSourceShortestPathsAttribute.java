package org.gradoop.flink.algorithms.gelly.shortestpaths.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Stores the minimum distance as a property in vertex.
 */
public class SingleSourceShortestPathsAttribute 
	implements JoinFunction<org.apache.flink.graph.Vertex<GradoopId, Double>, Vertex, Vertex> {

	/**
	 * Property to store the minimum distance in.
	 */
	private final String shortestPathProperty;
	
	public SingleSourceShortestPathsAttribute(String shortestPathProperty) {
		this.shortestPathProperty = shortestPathProperty;
	}
	
	@Override
	public Vertex join(org.apache.flink.graph.Vertex<GradoopId, Double> gellyVertex,
			Vertex gradoopVertex) 
		{
	    gradoopVertex.setProperty(shortestPathProperty, gellyVertex.getValue());
	    return gradoopVertex;
	  }
}
