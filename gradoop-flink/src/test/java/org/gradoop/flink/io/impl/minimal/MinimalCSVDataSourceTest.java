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
package org.gradoop.flink.io.impl.minimal;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class MinimalCSVDataSourceTest extends GradoopFlinkTestBase {

  @Test
  public void testMinimalRead() throws Exception {

	  String delimiter = ";";

	  String vertexPathA = MinimalCSVDataSourceTest.class
    	      .getResource("/data/minimal/input/A.csv").getFile();
	  String vertexPathB = MinimalCSVDataSourceTest.class
    	      .getResource("/data/minimal/input/B.csv").getFile();

	  List<String> vertexPropertiesA = Arrays.asList("a", "b", "c");
      List<String> vertexPropertiesB = Arrays.asList("a", "b", "c");

	  String edgePathA = MinimalCSVDataSourceTest.class
    	      .getResource("/data/minimal/input/a.csv").getFile();
	  String edgePathB = MinimalCSVDataSourceTest.class
    	      .getResource("/data/minimal/input/b.csv").getFile();

      List<String> edgePropertiesA = Arrays.asList("a", "b");
      List<String> edgePropertiesB = Arrays.asList("a");

	  String gdlFile = MinimalCSVDataSourceTest.class
    	      .getResource("/data/minimal/expected/expected.gdl").getFile();

	  Map<String, List<String>> pathToProperties = new HashMap<>();
	  pathToProperties.put(vertexPathA, vertexPropertiesA);
	  pathToProperties.put(vertexPathB, vertexPropertiesB);

	  Map<String, List<String>> pathToPropertiesEdge = new HashMap<>();
	  pathToPropertiesEdge.put(edgePathA, edgePropertiesA);
	  pathToPropertiesEdge.put(edgePathB, edgePropertiesB);

	  MinimalCSVVertexProvider vp = new MinimalCSVVertexProvider(pathToProperties, delimiter, getConfig());

	  MinimalCSVEdgeProvider ep = new MinimalCSVEdgeProvider(pathToPropertiesEdge, delimiter, getConfig());

	  DataSource source = new MinimalCSVDataSource(vp, ep, getConfig());

	  LogicalGraph input = source.getLogicalGraph();

	  FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);
	    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

	  collectAndAssertTrue(expected.equalsByElementData(input));
  }
}
