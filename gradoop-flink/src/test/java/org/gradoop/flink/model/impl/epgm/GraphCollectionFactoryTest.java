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
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for class {@link GraphCollectionFactory}
 */
public class GraphCollectionFactoryTest extends GradoopFlinkTestBase {
  FlinkAsciiGraphLoader loader;
  GraphCollectionFactory factory;

  @Before
  public void setUp() {
    loader = getLoaderFromString(
      "g1:Community {title : \"Graphs\", memberCount : 23}[" +
        "(alice:User {age:23})-[:knows]->(bob:User)," +
        "(bob)-[e1:knows]->(eve:User)," +
        "]" +
        "g2:Community {title : \"Databases\", memberCount : 42}[" +
        "(alice:User {age:23)" +
        "(peter:User {age:28)" +
        "(alice)-[e2:hates]->(peter)" +
        "]");

    factory = getConfig().getGraphCollectionFactory();
  }

  @Test
  public void testFromGraphMethod() throws Exception {
    collectAndAssertTrue(factory.fromGraph(loader.getLogicalGraphByVariable("g1"))
      .equalsByGraphElementData(loader.getGraphCollectionByVariables("g1")));
  }

  @Test
  public void testFromGraphsMethod() throws Exception {
    collectAndAssertTrue(
      factory.createEmptyCollection().equalsByGraphElementData(factory.fromGraphs()));

    collectAndAssertTrue(factory
      .fromGraphs(loader.getLogicalGraphByVariable("g1"), loader.getLogicalGraphByVariable("g2")).
        equalsByGraphElementData(loader.getGraphCollection()));
  }
}
