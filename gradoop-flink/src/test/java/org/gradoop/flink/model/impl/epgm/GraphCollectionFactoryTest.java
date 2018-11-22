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

/**
 * Tests for class {@link GraphCollectionFactory}
 */
public class GraphCollectionFactoryTest extends GradoopFlinkTestBase {

  /**
   * Test converting a {@link LogicalGraph}, respectively a arbitrary number of those into a
   * {@link GraphCollection}
   *
   * @throws Exception
   */
  @Test
  public void testFromGrah() throws Exception {
    GraphCollectionFactory gcf = getConfig().getGraphCollectionFactory();

    LogicalGraph lg1 = getLoaderFromString(
      "g1:Community {title : \"Graphs\", memberCount : 23}[" +
        "(alice:User)-[:knows]->(bob:User)," +
        "(bob)-[e:knows]->(eve:User)," +
        "(eve)" +
        "]").getLogicalGraphByVariable("g1");

    LogicalGraph lg2 = getLoaderFromString(
      "g2:Community {title : \"Databases\", memberCount : 42}[" +
        "(alice:User)" +
        "(bob:User)-[e:knows]->(eve:User)" +
        "]").getLogicalGraphByVariable("g2");

    GraphCollection gc = getLoaderFromString(
      "g1:Community {title : \"Graphs\", memberCount : 23}[" +
        "(alice:User)-[:knows]->(bob:User)," +
        "(bob)-[e:knows]->(eve:User)," +
        "(eve)" +
        "]" +
        "g2:Community {title : \"Databases\", memberCount : 42}[" +
        "(alice)" +
        "(bob)-[e]->(eve)" +
        "]").getGraphCollectionByVariables("g1", "g2");

    GraphCollection gc1 = gcf.fromDataSets(lg1.getGraphHead(), lg1.getVertices(), lg1.getEdges());

    gcf.fromGraph(lg1, lg2).print();
    gc.print();

    collectAndAssertTrue(gcf.fromGraph(lg1).equalsByGraphData(gc1));
    collectAndAssertTrue(gcf.fromGraph(lg1, lg2).equalsByGraphData(gc));
  }
}
