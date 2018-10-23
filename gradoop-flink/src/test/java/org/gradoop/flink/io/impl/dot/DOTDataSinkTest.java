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
package org.gradoop.flink.io.impl.dot;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DOTDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {

    String gdlFile = getFilePath("/data/dot/input.gdl");

    // load from gdl
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);

    // load input graph
    LogicalGraph inputGraph = loader.getLogicalGraphByVariable("input");

    // create temp directory
    String tmpDir = temporaryFolder.getRoot().toString();

    final String dotFile = tmpDir + "/check.dot";

    // create data sink
    DataSink dataSink = new DOTDataSink(dotFile, true);

    // write graph
    dataSink.write(inputGraph);

    // execute
    getExecutionEnvironment().execute();

    int graphLines = 0;
    int lines = 0;
    int subgraphLines = 0;
    int vertexLines = 0;
    int edgeLines = 0;

    // read written file
    List<String> dotLines = getExecutionEnvironment()
      .readTextFile(dotFile)
      .setParallelism(1) // force reading lines in order
      .collect();

    // count vertex and edge lines
    for (String line : dotLines) {

      if(line.contains("digraph") && lines == 0) {
        graphLines++;
      } else  if (line.contains("->")) {
        edgeLines++;
      } else if (line.startsWith("v")) {
        vertexLines++;
      } else if (line.startsWith("subgraph")) {
        subgraphLines++;
      }
      int index = line.indexOf('&');
      if (index != -1) {
        assertTrue("HTML entity & should have been escaped", line.substring(index).startsWith("&amp;"));
      }
      lines++;
    }

    // assert
    assertEquals("Wrong prefix/missing 'digraph'", 1, graphLines);
    assertEquals("Wrong number of subgraph lines", 1, subgraphLines);
    assertEquals("Wrong number of graph lines", 1, graphLines);
    assertEquals("Wrong number of edge lines", 4, edgeLines);
    assertEquals("Wrong number of vertex lines", 3, vertexLines);
 }
}
