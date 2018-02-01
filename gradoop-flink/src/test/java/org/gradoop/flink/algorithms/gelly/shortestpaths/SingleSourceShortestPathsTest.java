package org.gradoop.flink.algorithms.gelly.shortestpaths;

import java.util.List;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class SingleSourceShortestPathsTest extends GradoopFlinkTestBase {

	public TransformationFunction<Vertex> transformVertex;
	
	@Test
	public void testByElementData() throws Exception {
		
		String graphs = "input[" +
		"(v0 {id:0, vertexValue:0.0d})" +
		"(v1 {id:1, vertexValue:NULL})" +
		"(v2 {id:2, vertexValue:NULL})" +
		"(v3 {id:3, vertexValue:NULL})" +
		"(v4 {id:4, vertexValue:NULL})" +
		"(v0)-[e0 {edgeValue:2.0d}]->(v1)" +
		"(v0)-[e1 {edgeValue:7.0d}]->(v2)" +
		"(v0)-[e2 {edgeValue:6.0d}]->(v3)" +
		"(v1)-[e3 {edgeValue:3.0d}]->(v2)" +
		"(v1)-[e4 {edgeValue:6.0d}]->(v4)" +
		"(v2)-[e5 {edgeValue:5.0d}]->(v4)" +
		"(v3)-[e6 {edgeValue:1.0d}]->(v4)" +
		"]" +
		"result[" +
		"(v5 {id:0, vertexValue:0.0d})" +
		"(v6 {id:1, vertexValue:2.0d})" +
		"(v7 {id:2, vertexValue:5.0d})" +
		"(v8 {id:3, vertexValue:6.0d})" +
		"(v9 {id:4, vertexValue:7.0d})" +
		"(v5)-[e0 {edgeValue:2.0d}]->(v6)" +
		"(v5)-[e1 {edgeValue:7.0d}]->(v7)" +
		"(v5)-[e2 {edgeValue:6.0d}]->(v8)" +
		"(v6)-[e3 {edgeValue:3.0d}]->(v7)" +
		"(v6)-[e4 {edgeValue:6.0d}]->(v9)" +
		"(v7)-[e5 {edgeValue:5.0d}]->(v9)" +
		"(v8)-[e6 {edgeValue:1.0d}]->(v9)" +
		"]";
		
		FlinkAsciiGraphLoader loader = getLoaderFromString(graphs);
		LogicalGraph input = loader.getLogicalGraphByVariable("input");
		Vertex srcVertex = loader.getVertexByVariable("v0");
		GradoopId srcVertexId = srcVertex.getId();
		
		LogicalGraph outputGraph = input.callForGraph(new SingleSourceShortestPaths(srcVertexId, "edgeValue", 10, "vertexValue"));
		LogicalGraph expect = loader.getLogicalGraphByVariable("result");
		
		
	
		collectAndAssertTrue(outputGraph.equalsByElementData(expect));
		
	}
}
