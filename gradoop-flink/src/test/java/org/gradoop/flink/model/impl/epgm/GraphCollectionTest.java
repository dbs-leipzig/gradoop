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
package org.gradoop.flink.model.impl.epgm;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

public class GraphCollectionTest extends GradoopFlinkTestBase {

  @Test
  public void testFromGrah() throws Exception {
    GraphCollectionFactory gcf = getConfig().getGraphCollectionFactory();

    LogicalGraph lg1 = getLoaderFromString(
      "g1:Community {title : \"Graphs\", memberCount : 23}[" +
        "    (alice:User)-[:knows]->(bob:User)," +
        "    (bob)-[e:knows]->(eve:User)," +
        "    (eve)" +
        "]").getLogicalGraph();

    LogicalGraph lg2 = getLoaderFromString(
      "g2:Community {title : \"Databases\", memberCount : 42}[" +
        "    (alice)" +
        "]").getLogicalGraph();

    GraphCollection gc1 = gcf.fromDataSets(lg1.getGraphHead(), lg1.getVertices(), lg1.getEdges());
    GraphCollection gc2 = gcf.fromDataSets(lg2.getGraphHead(), lg2.getVertices(), lg2.getEdges());
    GraphCollection gcCombined = gc1.union(gc2);

    collectAndAssertTrue(gcf.fromGraph(lg1).equalsByGraphData(gc1));
    collectAndAssertTrue(gcf.fromGraph(lg1, lg2).equalsByGraphData(gcCombined));
  }
}
