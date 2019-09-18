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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Assert;
import org.junit.Test;

public class EdgeLengthDerivationTest extends GradoopFlinkTestBase {
  // a special graph with a known fixed layout
  static final String graph =
    "g1:graph[" + "(p1:Person {name: \"Bob\", age: 24, X: 57, Y: 0})-[:friendsWith]->" +
      "(p2:Person{name: \"Alice\", age: 30, X:316, Y: 0})-[:friendsWith]->(p1)" +
      "(p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27, X: 186, Y: 0})" +
      "-[:friendsWith]->(p2) " +
      "(p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40, X: 382, Y: 120})" +
      "-[:friendsWith]->(p3) " +
      "(p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33, X: 583, Y: 153})" +
      "-[:friendsWith]->(p4) " + "(c1:Company {name: \"Acme Corp\", X: 599, Y: 0}) " +
      "(c2:Company {name: \"Globex Inc.\", X: 0, Y: 0}) " + "(p2)-[:worksAt]->(c1) " +
      "(p4)-[:worksAt]->(c1) " + "(p5)-[:worksAt]->(c1) " + "(p1)-[:worksAt]->(c2) " +
      "(p3)-[:worksAt]->(c2) " + "] " + "g2:graph[" +
      "(p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37, X: 124, Y: 164})" +
      "-[:friendsWith]->(p4) " +
      "(p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23, X: 190, Y: 76})" +
      "-[:friendsWith]->(p6) " +
      "(p8:Person {name: \"Jil\", age: 32, X: 434, Y: 0})-[:friendsWith]->(p7)-[:friendsWith]->" +
      "(p8) " + "(p6)-[:worksAt]->(c2) " + "(p7)-[:worksAt]->(c2) " + "(p8)-[:worksAt]->(c1) " +
      "]";

  @Test
  public void edgeLenghthDerivationTest() throws Exception {

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());
    loader.initDatabaseFromString(graph);

    LogicalGraph g = loader.getLogicalGraph();

    EdgeLengthDerivation eld = new EdgeLengthDerivation();
    DataSet<Tuple2<Double, Double>> resds = eld.execute(g);
    Double result = resds.collect().get(0).f1;


    Assert.assertEquals(0.3047125465010722, result, 0.00001);
    System.out.println(result);

  }
}
