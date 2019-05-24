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
package org.gradoop.flink.io.impl.dot;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DOTDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private String dotFilePath;

  @Before
  public void initDotFilePath() {
    // create temp directory
    String tmpDir = temporaryFolder.getRoot().toString();

    dotFilePath = tmpDir + "/check.dot";
  }

  /**
   * Tests {@link DOTDataSink#write(LogicalGraph)} with instance that uses HTML tables.
   *
   * @throws Exception if something goes wrong.
   */
  @Test
  public void testWriteWithHtmlFormat() throws Exception {

    // create data sink
    DataSink dataSink = initDotDataSink(DOTDataSink.DotFormat.HTML);

    LogicalGraph inputGraph = initInputGraph();

    // write graph
    dataSink.write(inputGraph);

    // execute
    getExecutionEnvironment().execute();

    List<String> lines = readLinesFromEnv();

    checkWriteOutput(lines, DOTDataSink.DotFormat.HTML);
  }

  /**
   * Tests {@link DOTDataSink#write(LogicalGraph)} method with instance that uses plain dot
   * formatting.
   *
   * @throws Exception if something goes wrong.
   */
  @Test
  public void testWriteWithSimpleFormat() throws Exception {
    LogicalGraph inputGraph = initInputGraph();

    DataSink dotDataSink = initDotDataSink(DOTDataSink.DotFormat.SIMPLE);

    dotDataSink.write(inputGraph);

    getExecutionEnvironment().execute();

    List<String> lines = readLinesFromEnv();

    checkWriteOutput(lines, DOTDataSink.DotFormat.SIMPLE);
  }

  private LogicalGraph initInputGraph() throws Exception {
    String gdlFile = getFilePath("/data/dot/input.gdl");
    // load from gdl
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);
    // load input graph
    return loader.getLogicalGraphByVariable("input");
  }

  private DOTDataSink initDotDataSink(DOTDataSink.DotFormat format) {
    return new DOTDataSink(dotFilePath, true, format);
  }

  private List<String> readLinesFromEnv() throws Exception {
    return getExecutionEnvironment()
      .readTextFile(dotFilePath)
      .setParallelism(1) // force reading lines in order
      .collect();
  }

  private void checkWriteOutput(List<String> lines, DOTDataSink.DotFormat format) {
    int countGraphLines = 0;
    int countLines = 0;
    int countSubgraphLines = 0;
    int countVertexLines = 0;
    int countEdgeLines = 0;

    // count vertex and edge lines
    for (String line : lines) {

      if (line.contains("digraph") && countLines == 0) {
        countGraphLines++;
      } else  if (line.contains("->")) {
        countEdgeLines++;
      } else if (line.startsWith("v")) {
        countVertexLines++;
      } else if (line.startsWith("subgraph")) {
        countSubgraphLines++;
      }

      if (format == DOTDataSink.DotFormat.HTML) {
        int index = line.indexOf('&');
        if (index != -1) {
          assertTrue("HTML entity & should have been escaped", line.substring(index).startsWith("&amp;"));
        }
      }
      countLines++;
    }

    // assert
    assertEquals("Wrong prefix/missing 'digraph'", 1, countGraphLines);
    assertEquals("Wrong number of subgraph lines", 1, countSubgraphLines);
    assertEquals("Wrong number of graph lines", 1, countGraphLines);
    assertEquals("Wrong number of edge lines", 4, countEdgeLines);
    assertEquals("Wrong number of vertex lines", 3, countVertexLines);
  }
}
