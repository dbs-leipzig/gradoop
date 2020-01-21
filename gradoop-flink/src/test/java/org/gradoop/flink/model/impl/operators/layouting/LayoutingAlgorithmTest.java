/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Assert;
import org.junit.Test;

public abstract class LayoutingAlgorithmTest extends GradoopFlinkTestBase {

  public abstract LayoutingAlgorithm getLayouter(int w, int h);

  public static final String graph =
    "g1:graph[" + "(p1:Person {name: \"Bob\", age: 24})-[:friendsWith]->" +
      "(p2:Person{name: \"Alice\", age: 30})-[:friendsWith]->(p1)" +
      "(p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27})-[:friendsWith]->(p2) " +
      "(p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40})-[:friendsWith]->(p3) " +
      "(p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33})-[:friendsWith]->(p4) " +
      "(c1:Company {name: \"Acme Corp\"}) " + "(c2:Company {name: \"Globex Inc.\"}) " +
      "(p2)-[:worksAt]->(c1) " + "(p4)-[:worksAt]->(c1) " + "(p5)-[:worksAt]->(c1) " +
      "(p1)-[:worksAt]->(c2) " + "(p3)-[:worksAt]->(c2) " + "] " + "g2:graph[" +
      "(p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37})-[:friendsWith]->(p4) " +
      "(p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23})-[:friendsWith]->(p6) " +
      "(p8:Person {name: \"Jil\", age: 32})-[:friendsWith]->(p7)-[:friendsWith]->(p8) " +
      "(p6)-[:worksAt]->(c2) " + "(p7)-[:worksAt]->(c2) " + "(p8)-[:worksAt]->(c1) " + "]";

  @Test
  public void testLayouting() throws Exception {

    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    loader.initDatabaseFromString(graph);

    int width = 600;
    int height = 600;

    LayoutingAlgorithm l = getLayouter(width, height);
    LogicalGraph layouted = l.execute(loader.getLogicalGraph());

    long incorrectVertexes = layouted.getVertices().filter(new FilterFunction<EPGMVertex>() {
      public boolean filter(EPGMVertex value) throws Exception {
        if (!value.hasProperty(LayoutingAlgorithm.X_COORDINATE_PROPERTY) ||
          !value.hasProperty(LayoutingAlgorithm.Y_COORDINATE_PROPERTY)) {
          return true;
        }
        int x = value.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
        int y = value.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
        if (x < 0 || x > width || y < 0 || y > height) {
          return true;
        }
        return false;
      }
    }).count();

    Assert.assertEquals(0, incorrectVertexes);
  }
}
