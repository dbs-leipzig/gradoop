/**
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
package org.gradoop.flink.io.impl.minimal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.VertexLabeledEdgeListDataSourceTest;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class MinimalDataSourceTest extends GradoopFlinkTestBase {

  @Test
  public void testMinimalRead() throws Exception {

	  String delimiter = ";";

	  String vertexPathA = MinimalDataSourceTest.class
    	      .getResource("/data/minimal/input/A.csv").getFile();
	  String vertexPathB = MinimalDataSourceTest.class
    	      .getResource("/data/minimal/input/B.csv").getFile();
	  String vertexPropA1 = "a";
	  String vertexPropA2 = "b";
	  String vertexPropA3 = "c";

	  String vertexPropB1 = "a";
	  String vertexPropB2 = "b";
	  String vertexPropB3 = "c";
	  ArrayList<String> vertexPropertiesA = new ArrayList<>();
	  ArrayList<String> vertexPropertiesB = new ArrayList<>();
	  vertexPropertiesA.add(vertexPropA1);
	  vertexPropertiesA.add(vertexPropA2);
	  vertexPropertiesA.add(vertexPropA3);

	  vertexPropertiesB.add(vertexPropB1);
	  vertexPropertiesB.add(vertexPropB2);
	  vertexPropertiesB.add(vertexPropB3);

	  String edgePathA = MinimalDataSourceTest.class
    	      .getResource("/data/minimal/input/a.csv").getFile();
	  String edgePathB = MinimalDataSourceTest.class
    	      .getResource("/data/minimal/input/b.csv").getFile();
	  String aProp1 = "a";
	  String aProp2 = "b";

	  String bProp1 = "a";
	  ArrayList<String> aProperties = new ArrayList<>();
	  ArrayList<String> bProperties = new ArrayList<>();
	  aProperties.add(aProp1);
	  aProperties.add(aProp2);

	  bProperties.add(bProp1);

	  String gdlFile = VertexLabeledEdgeListDataSourceTest.class
    	      .getResource("/data/minimal/expected/expected.gdl").getFile();

	  Map<String, List<String>> pathToProperties = new HashMap<>();
	  pathToProperties.put(vertexPathA, vertexPropertiesA);
	  pathToProperties.put(vertexPathB, vertexPropertiesB);

	  Map<String, List<String>> pathToPropertiesEdge = new HashMap<>();
	  pathToPropertiesEdge.put(edgePathA, aProperties);
	  pathToPropertiesEdge.put(edgePathB, bProperties);

	  MinimalVertexProvider vp = new MinimalVertexProvider(pathToProperties, delimiter, getConfig());

	  MinimalEdgeProvider ep = new MinimalEdgeProvider(pathToPropertiesEdge, delimiter, getConfig());

	  DataSource source = new MinimalDataSource(getConfig(), vp, ep);

	  LogicalGraph input = source.getLogicalGraph();

	  FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);
	    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

	  collectAndAssertTrue(expected.equalsByElementData(input));
  }
}
