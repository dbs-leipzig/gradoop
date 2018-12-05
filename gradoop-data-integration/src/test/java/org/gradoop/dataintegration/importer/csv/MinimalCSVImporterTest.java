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
package org.gradoop.dataintegration.importer.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.importer.impl.csv.MinimalCSVImporter;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test class for {@link MinimalCSVImporter}
 */
public class MinimalCSVImporterTest extends GradoopFlinkTestBase {

  /**
   * Test if the properties of the imported vertices works correct. Set the first line
   * of the file as the column property names.
   *
   * @throws Exception
   */
  @Test
  public void testImportVertexWithHeader() throws Exception {

    String csvPath = getFilePath("/csv/input.csv");
    String delimiter = ";";

    MinimalCSVImporter importVertexImporter =
      new MinimalCSVImporter(csvPath, delimiter, getConfig(), true);

    //check the rows for the header line
    DataSet<ImportVertex<Long>> importVertex = importVertexImporter.importVertices();

    List<ImportVertex<Long>> lv = new ArrayList<>();
    importVertex.output(new LocalCollectionOutputFormat<>(lv));

    getExecutionEnvironment().execute();

    assertImportCSV(lv);
  }

  /**
   * Test if the properties of the imported vertices works correct. In this case
   * the file do not contain a header row. For this the user set a list with
   * the column names.
   *
   * @throws Exception
   */
  @Test
  public void testImportVertexWithoutHeader() throws Exception {

    String csvPath = getFilePath("/csv/inputWithoutHeader.csv");
    String delimiter = ";";

    List<String> columnNames = Arrays.asList("name", "value1", "value2", "value3");
    MinimalCSVImporter importVertexImporter =
      new MinimalCSVImporter(csvPath, delimiter, getConfig(), columnNames, false);
    DataSet<ImportVertex<Long>> importVertex = importVertexImporter.importVertices();

    List<ImportVertex<Long>> lv = new ArrayList<>();
    importVertex.output(new LocalCollectionOutputFormat<>(lv));

    getExecutionEnvironment().execute();

    assertImportCSV(lv);
  }

  /**
   * Test if the import and creation of a logical graph works correct. Set the first line
   * of the file as the column property names.
   *
   * @throws Exception
   */
  @Test
  public void testImportLogicalGraphWithHeader() throws Exception {

    String csvPath = getFilePath("/csv/input.csv");
    String delimiter = ";";

    String gdlFile = getFilePath("/csv/expected.gdl");

    MinimalCSVImporter importVertexImporter =
      new MinimalCSVImporter(csvPath, delimiter, getConfig(), true);

    LogicalGraph result = importVertexImporter.getLogicalGraph();
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    collectAndAssertTrue(expected.equalsByElementData(result));
  }

  /**
   * Test if the import and creation of a logical graph works correct. In this case
   * the file do not contain a header row. For this the user set a list with
   * the column names.
   *
   * @throws Exception
   */
  @Test
  public void testImportLogicalGraphWithoutHeader() throws Exception {

    String csvPath = getFilePath("/csv/inputWithoutHeader.csv");
    String delimiter = ";";

    String gdlFile = getFilePath("/csv/expected.gdl");

    List<String> columnNames = Arrays.asList("name", "value1", "value2", "value3");
    MinimalCSVImporter importVertexImporter =
      new MinimalCSVImporter(csvPath, delimiter, getConfig(), columnNames, false);

    LogicalGraph result = importVertexImporter.getLogicalGraph();
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    collectAndAssertTrue(expected.equalsByElementData(result));
  }

  /**
   * Test if the reoccurring header flag is set this row will skipped.
   * @throws Exception
   */
  @Test
  public void testReoccurringHeader() throws Exception {

    String csvPathWithHeader = getFilePath("/csv/input.csv");
    String csvPathWithoutHeader = getFilePath("/csv/inputWithoutHeader.csv");
    String delimiter = ";";

    //set the first line of the file as column property names and check file of reoccurring and skip
    //header row as vertex
    MinimalCSVImporter importVertexImporterHeader =
      new MinimalCSVImporter(csvPathWithHeader, delimiter, getConfig(), true);

    //read all rows of the csv files as vertices
    List<String> columnNames = Arrays.asList("name", "value1", "value2", "value3");
    MinimalCSVImporter importVertexImporter =
      new MinimalCSVImporter(csvPathWithoutHeader, delimiter, getConfig(), columnNames, false);

    LogicalGraph resultWithHeader = importVertexImporter.getLogicalGraph();
    LogicalGraph resultWithoutHeader = importVertexImporter.getLogicalGraph();

    collectAndAssertTrue(resultWithHeader.equalsByElementData(resultWithoutHeader));
  }

  /**
   * Test if an empty line in the csv file will be skipped.
   * @throws Exception
   */
  @Test
  public void testEmptyLines() throws Exception {

    String csvPath = getFilePath("/csv/inputEmptyLines.csv");
    String delimiter = ";";

    String gdlFile = getFilePath("/csv/expected.gdl");

    MinimalCSVImporter importVertexImporter =
      new MinimalCSVImporter(csvPath, delimiter, getConfig(), true);

    LogicalGraph result = importVertexImporter.getLogicalGraph();
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    collectAndAssertTrue(expected.equalsByElementData(result));
  }

  /**
   * Test if a row contains empty entries, no property will be create.
   * @throws Exception
   */
  @Test
  public void testEmptyProperty() throws Exception {

    String csvPath = getFilePath("/csv/inputEmptyPropertyValues.csv");
    String delimiter = ";";

    MinimalCSVImporter importVertexImporter =
      new MinimalCSVImporter(csvPath, delimiter, getConfig(), true);

    LogicalGraph result = importVertexImporter.getLogicalGraph();

    List<Vertex> lv = new ArrayList<>();
    result.getVertices().output(new LocalCollectionOutputFormat<>(lv));

    getExecutionEnvironment().execute();

    for (Vertex v : lv) {
      if (v.hasProperty("name")) {
        assertEquals(2, v.getPropertyCount());
        assertEquals(v.hasProperty("value1"), false);
        assertEquals(v.hasProperty("value2"), true);
        assertEquals(v.hasProperty("value3"), false);
      } else if (v.hasProperty("value1")) {
        assertEquals(2, v.getPropertyCount());
        assertEquals(v.hasProperty("name"), false);
        assertEquals(v.hasProperty("value2"), false);
        assertEquals(v.hasProperty("value3"), true);
      } else {
        fail();
      }
    }

  }

  /**
   * Checks if the csv file import is equals to the assert import.
   *
   * @param lv list of the import vertices
   */
  public void assertImportCSV(List<ImportVertex<Long>> lv) {
    assertEquals(3, lv.size());

    for (ImportVertex<Long> v : lv) {
      if (v.f2.get("name").toString().equals("foo")) {
        assertEquals(4, v.f2.size());
        assertEquals("453", v.f2.get("value1").toString());
        assertEquals("true", v.f2.get("value2").toString());
        assertEquals("71.03", v.f2.get("value3").toString());
      } else if (v.f2.get("name").toString().equals("bar")) {
        assertEquals(4, v.f2.size());
        assertEquals("76", v.f2.get("value1").toString());
        assertEquals("false", v.f2.get("value2").toString());
        assertEquals("33.4", v.f2.get("value3").toString());
      } else if (v.f2.get("name").toString().equals("bla")) {
        assertEquals(3, v.f2.size());
        assertEquals("4568", v.f2.get("value1").toString());
        assertEquals("9.42", v.f2.get("value3").toString());
      } else {
        fail();
      }
    }
  }
}
