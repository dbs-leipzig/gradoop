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
package org.gradoop.temporal.io.impl.csv.indexed;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Test class to test {@link TemporalIndexedCSVDataSink}.
 */
public class TemporalIndexedCSVDataSinkTest extends TemporalGradoopTestBase {

  /**
   * Temporal graph to test
   */
  private TemporalGraph testGraph;

  /**
   * Temporary test folder to write the test graph.
   */
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  /**
   * Creates a test temporal graph from the social network loader.
   *
   * @throws Exception if loading the graph fails
   */
  @Before
  public void setUp() throws Exception {
    testGraph = toTemporalGraph(getSocialNetworkLoader().getLogicalGraph())
      .transformGraphHead((current, trans) -> {
        current.setProperty("testGraphHeadProperty", PropertyValue.create(1L));
        return current;
      });
  }

  /**
   * Test writing a temporal graph to an indexed csv sink.
   *
   * @throws Exception in case of a write error
   */
  @Test
  public void testWrite() throws Exception {
    File file = testFolder.newFolder();
    String tempFolderPath = file.getPath();

    testGraph.writeTo(new TemporalIndexedCSVDataSink(tempFolderPath, getConfig()));
    getExecutionEnvironment().execute();

    final File[] subdirs = file.listFiles(File::isDirectory);

    assertNotNull(subdirs);
    assertEquals(3, subdirs.length);
    final List<File> files = Arrays.asList(subdirs);
    final List<String> subDirList = files.stream().map(File::getName).collect(Collectors.toList());
    assertTrue(subDirList.contains("vertices"));
    assertTrue(subDirList.contains("edges"));
    assertTrue(subDirList.contains("graphs"));

    for (File elementDir : files) {
      if (elementDir.getName().equals("vertices")) {
        final File[] vertexLabelArray = elementDir.listFiles(File::isDirectory);
        assertNotNull(vertexLabelArray);
        final List<String> vertexLabelDirs = Arrays.stream(vertexLabelArray).map(File::getName)
          .collect(Collectors.toList());
        assertNotNull(vertexLabelDirs);
        assertEquals(3, vertexLabelDirs.size());
        assertTrue(vertexLabelDirs.contains("person"));
        assertTrue(vertexLabelDirs.contains("forum"));
        assertTrue(vertexLabelDirs.contains("tag"));

      } else if (elementDir.getName().equals("edges")) {
        final File[] edgeLabelArray = elementDir.listFiles(File::isDirectory);
        assertNotNull(edgeLabelArray);
        final List<String> edgeLabelDirs = Arrays.stream(edgeLabelArray).map(File::getName)
          .collect(Collectors.toList());
        assertNotNull(edgeLabelDirs);
        assertEquals(5, edgeLabelDirs.size());
        assertTrue(edgeLabelDirs.contains("hasmember"));
        assertTrue(edgeLabelDirs.contains("hasinterest"));
        assertTrue(edgeLabelDirs.contains("knows"));
        assertTrue(edgeLabelDirs.contains("hasmoderator"));
        assertTrue(edgeLabelDirs.contains("hastag"));

      } else if (elementDir.getName().equals("graphs")) {
        final File[] graphLabelArray = elementDir.listFiles(File::isDirectory);
        assertNotNull(graphLabelArray);
        final List<String> graphLabelDirs = Arrays.stream(graphLabelArray).map(File::getName)
          .collect(Collectors.toList());
        assertNotNull(graphLabelDirs);
        assertEquals(1, graphLabelDirs.size());
        assertTrue(graphLabelDirs.contains("_db"));
      }
    }
  }

  /**
   * Test the {@link TemporalIndexedCSVDataSink#write(TemporalGraph)} method as well as the
   * {@link TemporalIndexedCSVDataSource}.
   *
   * @throws Exception in case of a write error
   */
  @Test
  public void testWriteAndReadAfterwards() throws Exception {
    File file = testFolder.newFolder();
    String tempFolderPath = file.getPath();
    new TemporalIndexedCSVDataSink(tempFolderPath, getConfig()).write(testGraph);
    getExecutionEnvironment().execute();
    TemporalDataSource dataSource = new TemporalIndexedCSVDataSource(tempFolderPath, getConfig());
    collectAndAssertTrue(dataSource.getTemporalGraph().equalsByData(testGraph));
  }
}
