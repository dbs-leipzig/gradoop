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
package org.gradoop.dataintegration.importer.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Test class for {@link MinimalCSVImporter}
 */
public class MinimalCSVImporterTest extends GradoopFlinkTestBase {
  /**
   * Used delimiter
   */
  private static final String DELIMITER = ";";

  /**
   * Test if the property import of the vertices works correct. Set the first line of the file as
   * the column property names.
   *
   * @throws Exception on failure
   */
  @Test
  public void testImportVertexWithHeader() throws Exception {
    String csvPath = MinimalCSVImporterTest.class.getResource("/csv/input.csv").getPath();

    DataSource importVertexImporter = new MinimalCSVImporter(csvPath, DELIMITER, getConfig(), true);

    DataSet<Vertex> importVertex = importVertexImporter.getLogicalGraph().getVertices();

    List<Vertex> lv = new ArrayList<>();
    importVertex.output(new LocalCollectionOutputFormat<>(lv));

    getExecutionEnvironment().execute();

    checkImportedVertices(lv);
  }

  /**
   * Test the retrieval of a graph collection
   *
   * @throws Exception on failure
   */
  @Test
  public void testImportGraphCollection() throws Exception {
    String csvPath = MinimalCSVImporterTest.class.getResource("/csv/input.csv").getPath();

    DataSource importVertexImporter = new MinimalCSVImporter(csvPath, DELIMITER, getConfig(), true);

    DataSet<Vertex> importVertex = importVertexImporter.getGraphCollection().getVertices();

    List<Vertex> lv = new ArrayList<>();
    importVertex.output(new LocalCollectionOutputFormat<>(lv));

    getExecutionEnvironment().execute();

    checkImportedVertices(lv);
  }

  /**
   * Test if the property import of the vertices works correct. In this case
   * the file do not contain a header row. For this the user set a list with
   * the column names.
   *
   * @throws Exception on failure
   */
  @Test
  public void testImportVertexWithoutHeader() throws Exception {
    String csvPath = MinimalCSVImporterTest.class
      .getResource("/csv/inputWithoutHeader.csv").getPath();

    List<String> columnNames = Arrays.asList("name", "value1", "value2", "value3");

    DataSource importer = new MinimalCSVImporter(csvPath, DELIMITER, getConfig(), columnNames, false);
    DataSet<Vertex> importVertex = importer.getLogicalGraph().getVertices();

    List<Vertex> lv = new ArrayList<>();
    importVertex.output(new LocalCollectionOutputFormat<>(lv));

    getExecutionEnvironment().execute();

    checkImportedVertices(lv);
  }

  /**
   * Test if the importer throws an exception if an empty file is given.
   *
   * @throws Exception on failure
   */
  @Test
  public void testImporterWithEmptyFile() throws Exception {
    // Create an empty file
    File emptyFile = File.createTempFile("empty-csv", null);

    DataSource importer =
      new MinimalCSVImporter(emptyFile.getPath(), DELIMITER, getConfig(), false);

    checkExceptions(importer, emptyFile);
  }

  /**
   * Test if the importer throws an exception if an file with an empty header row is given.
   *
   * @throws Exception on failure
   */
  @Test
  public void testImporterWithEmptyLineInFile() throws Exception {
    // Create an empty file and add an empty line
    File emptyFile = File.createTempFile("empty-csv", null);
    FileWriter fileWriter = new FileWriter(emptyFile, true);
    fileWriter.write(System.getProperty("line.separator"));
    fileWriter.close();

    DataSource importer =
      new MinimalCSVImporter(emptyFile.getPath(), DELIMITER, getConfig(), false);

    checkExceptions(importer, emptyFile);
  }

  /**
   * Test if the import and creation of a logical graph works correct. Set the first line
   * of the file as the column property names.
   *
   * @throws Exception on failure
   */
  @Test
  public void testImportLogicalGraphWithHeader() throws Exception {
    String csvPath = MinimalCSVImporterTest.class.getResource("/csv/input.csv").getPath();
    String gdlPath = MinimalCSVImporterTest.class.getResource("/csv/expected.gdl").getPath();

    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlPath);

    DataSource importVertexImporter = new MinimalCSVImporter(csvPath, DELIMITER, getConfig(), true);
    LogicalGraph resultGraph = importVertexImporter.getLogicalGraph();

    LogicalGraph expectedGraph = loader.getLogicalGraphByVariable("expected");

    collectAndAssertTrue(expectedGraph.equalsByElementData(resultGraph));
  }

  /**
   * Test if the import and creation of a logical graph works correct. In this case
   * the file do not contain a header row. For this the user set a list with
   * the column names.
   *
   * @throws Exception on failure
   */
  @Test
  public void testImportLogicalGraphWithoutHeader() throws Exception {
    String csvPath = MinimalCSVImporterTest.class
      .getResource("/csv/inputWithoutHeader.csv").getPath();

    String gdlPath = MinimalCSVImporterTest.class.getResource("/csv/expected.gdl").getPath();

    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlPath);

    List<String> columnNames = Arrays.asList("name", "value1", "value2", "value3");
    DataSource importVertexImporter =
      new MinimalCSVImporter(csvPath, DELIMITER, getConfig(), columnNames, false);
    LogicalGraph resultGraph = importVertexImporter.getLogicalGraph();

    LogicalGraph expectedGraph = loader.getLogicalGraphByVariable("expected");

    collectAndAssertTrue(expectedGraph.equalsByElementData(resultGraph));
  }

  /**
   * Test if the reoccurring header flag is working correctly.
   *
   * @throws Exception on failure
   */
  @Test
  public void testReoccurringHeader() throws Exception {
    String csvPathWithHeader = MinimalCSVImporterTest.class
      .getResource("/csv/input.csv").getPath();
    String csvPathWithoutHeader = MinimalCSVImporterTest.class
      .getResource("/csv/inputWithoutHeader.csv").getPath();

    //set the first line of the file as column property names and check file of reoccurring and skip
    //header row as vertex
    DataSource importerWithHeader =
      new MinimalCSVImporter(csvPathWithHeader, DELIMITER, getConfig(), true);

    //read all rows of the csv files as vertices
    List<String> columnNames = Arrays.asList("name", "value1", "value2", "value3");
    DataSource importerWithoutHeader =
      new MinimalCSVImporter(csvPathWithoutHeader, DELIMITER, getConfig(), columnNames, false);

    LogicalGraph resultWithHeader = importerWithHeader.getLogicalGraph();
    LogicalGraph resultWithoutHeader = importerWithoutHeader.getLogicalGraph();

    collectAndAssertTrue(resultWithHeader.equalsByElementData(resultWithoutHeader));
  }

  /**
   * Test if an empty line in the csv file will be skipped.
   *
   * @throws Exception on failure
   */
  @Test
  public void testEmptyLines() throws Exception {
    String csvPath = MinimalCSVImporterTest.class
      .getResource("/csv/inputEmptyLines.csv").getPath();

    String gdlPath = MinimalCSVImporterTest.class.getResource("/csv/expected.gdl").getPath();

    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlPath);

    DataSource importer = new MinimalCSVImporter(csvPath, DELIMITER, getConfig(), true);
    LogicalGraph result = importer.getLogicalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    collectAndAssertTrue(expected.equalsByElementData(result));
  }

  /**
   * Test if no property will be created if a row contains empty entries .
   *
   * @throws Exception on failure
   */
  @Test
  public void testEmptyProperty() throws Exception {
    String csvPath = MinimalCSVImporterTest.class
      .getResource("/csv/inputEmptyPropertyValues.csv").getPath();

    DataSource importer = new MinimalCSVImporter(csvPath, DELIMITER, getConfig(), true);
    LogicalGraph result = importer.getLogicalGraph();

    List<Vertex> lv = new ArrayList<>();
    result.getVertices().output(new LocalCollectionOutputFormat<>(lv));

    getExecutionEnvironment().execute();

    for (Vertex v : lv) {
      if (v.hasProperty("name")) {
        assertEquals(2, v.getPropertyCount());
        assertFalse(v.hasProperty("value1"));
        assertTrue(v.hasProperty("value2"));
        assertFalse(v.hasProperty("value3"));
      } else if (v.hasProperty("value1")) {
        assertEquals(2, v.getPropertyCount());
        assertFalse(v.hasProperty("name"));
        assertFalse(v.hasProperty("value2"));
        assertTrue(v.hasProperty("value3"));
      } else {
        fail();
      }
    }
  }

  /**
   * Checks if the csv file import is equals to the assert import.
   *
   * @param vertices list of the import vertices
   */
  private void checkImportedVertices(List<Vertex> vertices) {
    assertEquals(3, vertices.size());

    for (Vertex v : vertices) {
      switch (String.valueOf(v.getPropertyValue("name"))) {
      case "foo":
        assertNotNull(v.getProperties());
        assertEquals(4, v.getProperties().size());
        assertEquals("453", v.getPropertyValue("value1").getString());
        assertEquals("true", v.getPropertyValue("value2").getString());
        assertEquals("71.03", v.getPropertyValue("value3").getString());
        break;
      case "bar":
        assertNotNull(v.getProperties());
        assertEquals(4, v.getProperties().size());
        assertEquals("76", v.getPropertyValue("value1").getString());
        assertEquals("false", v.getPropertyValue("value2").getString());
        assertEquals("33.4", v.getPropertyValue("value3").getString());
        break;
      case "bla":
        assertNotNull(v.getProperties());
        assertEquals(3, v.getProperties().size());
        assertEquals("4568", v.getPropertyValue("value1").toString());
        assertFalse(v.hasProperty("value2"));
        assertEquals("9.42", v.getPropertyValue("value3").toString());
        break;
      default:
        fail();
        break;
      }
    }
  }

  /**
   * Check if the correct exceptions are thrown if the file of the importer is empty.
   *
   * @param importer the importer to check
   * @param emptyFile the file given to the importer
   */
  private void checkExceptions(DataSource importer, File emptyFile) {
    String exceptionMessage = "Error while opening a stream to '" + emptyFile.getPath() + "'.";
    String causeMessage = "The csv file '" + emptyFile.getPath() + "' does not contain any rows.";
    try {
      importer.getLogicalGraph();
      fail("No expected IOException was thrown.");
    } catch (IOException exception) {
      assertEquals(exceptionMessage, exception.getMessage());
      assertEquals(causeMessage, exception.getCause().getMessage());
    }
  }
}
