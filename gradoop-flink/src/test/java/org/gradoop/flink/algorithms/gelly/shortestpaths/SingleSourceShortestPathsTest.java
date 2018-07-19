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
package org.gradoop.flink.algorithms.gelly.shortestpaths;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class SingleSourceShortestPathsTest extends GradoopFlinkTestBase {
  @Test
  public void testByElementData() throws Exception {

    String graphsDouble = "input[" +
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
      "(v5)-[e7 {edgeValue:2.0d}]->(v6)" +
      "(v5)-[e8 {edgeValue:7.0d}]->(v7)" +
      "(v5)-[e9 {edgeValue:6.0d}]->(v8)" +
      "(v6)-[e10 {edgeValue:3.0d}]->(v7)" +
      "(v6)-[e11 {edgeValue:6.0d}]->(v9)" +
      "(v7)-[e12 {edgeValue:5.0d}]->(v9)" +
      "(v8)-[e13 {edgeValue:1.0d}]->(v9)" +
      "]";
    
    String graphs = "input[" +
        "(v0 {id:0, vertexValue:0.0})" +
        "(v1 {id:1, vertexValue:NULL})" +
        "(v2 {id:2, vertexValue:NULL})" +
        "(v3 {id:3, vertexValue:NULL})" +
        "(v4 {id:4, vertexValue:NULL})" +
        "(v0)-[e0 {edgeValue:2.0}]->(v1)" +
        "(v0)-[e1 {edgeValue:7.0}]->(v2)" +
        "(v0)-[e2 {edgeValue:6.0}]->(v3)" +
        "(v1)-[e3 {edgeValue:3.0}]->(v2)" +
        "(v1)-[e4 {edgeValue:6.0}]->(v4)" +
        "(v2)-[e5 {edgeValue:5.0}]->(v4)" +
        "(v3)-[e6 {edgeValue:1.0}]->(v4)" +
        "]" +
        "result[" +
        "(v5 {id:0, vertexValue:0.0d})" +
        "(v6 {id:1, vertexValue:2.0d})" +
        "(v7 {id:2, vertexValue:5.0d})" +
        "(v8 {id:3, vertexValue:6.0d})" +
        "(v9 {id:4, vertexValue:7.0d})" +
        "(v5)-[e7 {edgeValue:2.0}]->(v6)" +
        "(v5)-[e8 {edgeValue:7.0}]->(v7)" +
        "(v5)-[e9 {edgeValue:6.0}]->(v8)" +
        "(v6)-[e10 {edgeValue:3.0}]->(v7)" +
        "(v6)-[e11 {edgeValue:6.0}]->(v9)" +
        "(v7)-[e12 {edgeValue:5.0}]->(v9)" +
        "(v8)-[e13 {edgeValue:1.0}]->(v9)" +
        "]";

    //test a graph with double values as input
    FlinkAsciiGraphLoader loaderDouble = getLoaderFromString(graphsDouble);
    LogicalGraph inputDouble = loaderDouble.getLogicalGraphByVariable("input");
    Vertex srcVertexDouble = loaderDouble.getVertexByVariable("v0");
    GradoopId srcVertexIdDouble = srcVertexDouble.getId();

    LogicalGraph outputGraphDouble = inputDouble.callForGraph(new SingleSourceShortestPaths(srcVertexIdDouble,
      "edgeValue", 10, "vertexValue"));
    LogicalGraph expectDouble = loaderDouble.getLogicalGraphByVariable("result");
    
    collectAndAssertTrue(outputGraphDouble.equalsByElementData(expectDouble));

  //test a graph with float values as input
    FlinkAsciiGraphLoader loader = getLoaderFromString(graphs);
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    Vertex srcVertex = loader.getVertexByVariable("v0");
    GradoopId srcVertexId = srcVertex.getId();

    LogicalGraph outputGraph = input.callForGraph(new SingleSourceShortestPaths(srcVertexId,
      "edgeValue", 10, "vertexValue"));
    LogicalGraph expect = loader.getLogicalGraphByVariable("result");
    
    collectAndAssertTrue(outputGraph.equalsByElementData(expect));
  }
}
