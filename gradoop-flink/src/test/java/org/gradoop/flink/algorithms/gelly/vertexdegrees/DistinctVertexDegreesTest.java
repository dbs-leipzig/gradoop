/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.gelly.vertexdegrees;

import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class DistinctVertexDegreesTest extends GradoopFlinkTestBase {

	@Test
	public void testByElementData() throws Exception {
		
		String graph = "input[" +
		"(v0 {id:0})" +
		"(v1 {id:1})" +
		"(v2 {id:2})" +
		"(v3 {id:3})" +
		"(v0)-[e0]->(v1)" +
		"(v0)-[e1]->(v2)" +
		"(v2)-[e2]->(v3)" +
		"(v2)-[e3]->(v1)" +
		"(v3)-[e4]->(v2)" +
		"]" +
		"result[" +
		"(v4 {id:0, degree:2L, inDegree:0L, outDegree:2L})" +
		"(v5 {id:1, degree:2L, inDegree:2L, outDegree:0L})" +
		"(v6 {id:2, degree:3L, inDegree:2L, outDegree:2L})" +
		"(v7 {id:3, degree:1L, inDegree:1L, outDegree:1L})" +
		"(v4)-[e5]->(v5)" +
		"(v4)-[e6]->(v6)" +
		"(v6)-[e7]->(v7)" +
		"(v6)-[e8]->(v5)" +
		"(v7)-[e9]->(v6)" +
		"]";

		FlinkAsciiGraphLoader loader = getLoaderFromString(graph);
		LogicalGraph input = loader.getLogicalGraphByVariable("input");
		
		LogicalGraph outputGraph = input.callForGraph(new DistinctVertexDegrees("degree", "inDegree", "outDegree", false));
		LogicalGraph expect = loader.getLogicalGraphByVariable("result");
    
		collectAndAssertTrue(outputGraph.equalsByElementData(expect));
	}
}
