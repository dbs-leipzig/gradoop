/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.grouping;

import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class GroupingGroupReduceLabelsTest extends GradoopFlinkTestBase {

  // TODO test that vertices with multiple properties are cloned correctly
  // TODO create testcase: user is not using label specific grouping => keepVertices
  // should do nothing

  private static final String EXPECTED_FORUMS_NO_LABEL_GROUPING = "g1:graph[" +
    // all vertices
    "(f1 {title: \"Graph Processing\", count : 1L})" +
    "(f2 {title: \"Graph Databases\", count : 1L})" + "(ps:Person {count: 6L})" +
    "(ts:Tag {count: 3L})" +
    // all edges
    "(f1)-[:hasMember]->(ps)" + "(f1)-[:hasModerator]->(ps)" + "(f2)-[:hasMember]->(ps)" +
    "(f2)-[:hasModerator]->(ps)" + "(ps)-[:knows]->(ps)" + "(ps)-[:hasInterest]->(ts)" +
    "(f1)-[:hasTag]->(ts)" + "(f2)-[:hasTag]->(ts)" + "]";

  private static final String EXPECTED_FORUMS_AND_TAG_GRAPHS_NO_LABEL_GROUPING =
    "g1:graph[" + "(f1:noLabel_0 {title: \"Graph Processing\", count : 1L})" +
      "(f2:noLabel_1 {title: \"Graph Databases\", count : 1L})" + "(tg:noLabel_2 {name: " +
      "\"Graphs\", count :" + " 1L})" + "(ps:Person {count: 6L})" + "(ts:Tag {count: 2L})" +
      "(f1)-[:hasMember]->(ps)" + "(f1)-[:hasModerator]->(ps)" + "(f2)-[:hasMember]->(ps)" +
      "(f2)-[:hasModerator]->(ps)" + "(ps)-[:knows]->(ps)" + "(ps)-[:hasInterest]->(ts)" +
      "(f1)-[:hasTag]->(tg)" + "(f2)-[:hasTag]->(tg)" + "(f1)-[:hasTag]->(ts)" +
      "(f2)-[:hasTag]->(ts)" + "]";


  protected static void convertDotToPNG(String dotFile, String pngFile) throws IOException {
    ProcessBuilder pb = new ProcessBuilder("dot", "-Tpng", dotFile);
    File output = new File(pngFile);
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(output));
    pb.start();


  }

  @Test
  public void testKeepVerticesFlag() {
    Grouping grouping = new Grouping.GroupingBuilder().setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setKeepVertices(true).build();

    assertTrue(grouping.isKeepingVertices());
  }

  @Test
  public void testGroupingWithoutLabel() throws Exception {

    System.out.println(
      "operators/GroupingGroupReduceLabelsTest.java, working directory:" + new File("out/").getAbsolutePath());

    LogicalGraph expectedGraph =
      getExpectedGraph(EXPECTED_FORUMS_AND_TAG_GRAPHS_NO_LABEL_GROUPING, true);

    // Actual Graph
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    String dotPath = "out/groupingTest.dot";

    boolean deleted = new File(dotPath + ".png").delete();
    System.out.println("deleted old file? " + deleted);

    DOTDataSink sink = new DOTDataSink(dotPath, true);

    LogicalGraph logicalGraph = loader.getLogicalGraph();

    logicalGraph = logicalGraph.transformVertices((vertex, cpy) -> {
//|| (vertex.getLabel().equals("Tag") &&
//        vertex.getPropertyValue("name").getString().equals("Graphs"))
      if (vertex.getLabel().equals("Forum") || (vertex.getLabel().equals("Tag") &&
        vertex.getPropertyValue("name").getString().equals("Graphs"))) {
        vertex.setLabel("");
      }
      return vertex;
    });

    logicalGraph = new Grouping.GroupingBuilder().setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setKeepVertices(true).useVertexLabel(true).useEdgeLabel(true)
      .addVertexAggregateFunction(new Count()).build().execute(logicalGraph);

    sink.write(logicalGraph, true);

    logicalGraph.print();

    // No data sinks have been created yet. A program needs at least one sink that consumes data.
    //env.execute();

    convertDotToPNG(dotPath, dotPath + ".png");


    System.out.println("finish");

    /*Optional<Boolean> reduce = logicalGraph.equalsByElementData(expectedGraph).collect().stream()
      .reduce(Boolean::logicalAnd);

    System.out.println("expected equals actual? " + reduce);*/

    collectAndAssertTrue(logicalGraph.equalsByElementData(expectedGraph));

  }

  private LogicalGraph getExpectedGraph(String asciiGraph, boolean write) {


    // Expected Graph read and write
    FlinkAsciiGraphLoader expectedGraphLoader = new FlinkAsciiGraphLoader(getConfig());
    expectedGraphLoader.initDatabaseFromString(asciiGraph);

    LogicalGraph expectedGraph = expectedGraphLoader.getLogicalGraphByVariable("g1");

    if (write) {

      final String expectedGraphPath = "out/expected.dot";
      new File(expectedGraphPath).delete();
      new File(expectedGraphPath+".png").delete();
      DOTDataSink dotDataSink = new DOTDataSink(expectedGraphPath, true);

      try {
        dotDataSink.write(expectedGraph, false);
        expectedGraph.print();
        convertDotToPNG(expectedGraphPath, expectedGraphPath + ".png");

        System.out.println("successful write");
      } catch (IOException e) {
        System.err.println("writing expected graph failed");
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    System.out.println("returning expected graph" + expectedGraph);
    return expectedGraph;
  }
}
