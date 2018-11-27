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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

/**
 * Test class for {@link org.gradoop.dataintegration.importer.csv.MinimalCSVImporter}
 */
public class MinimalCSVImporterTest extends GradoopFlinkTestBase {

  /**
   * Test if the properties of the imported vertices works correct. Set the first line
   * of the file as the column property names.
   * @throws Exception
   */
  @Test
  public void testImportWithHeader() throws Exception {

    String csvPath = getFilePath("/csv/input.csv");
    String delimiter = ";";

    ExecutionEnvironment env = getExecutionEnvironment();

    MinimalCSVImporter importVertexImporter =
            new MinimalCSVImporter(csvPath, delimiter, getConfig());

    //check the rows for the header line
    DataSet<ImportVertex<Long>> importVertex = importVertexImporter.importVertices(true);

    List<ImportVertex<Long>> lv = new ArrayList<>();
    importVertex.output(new LocalCollectionOutputFormat<>(lv));

    env.execute();

    assertThat(lv.size(), is(3));

    for (ImportVertex<Long> v : lv) {
      if (v.f2.get("name").toString().equals("foo")) {
        assertThat(v.f2.size(), is(4));
        assertThat(v.f2.get("name").toString(), is("foo"));
        assertThat(v.f2.get("value1").toString(), is("453"));
        assertThat(v.f2.get("value2").toString(), is("true"));
        assertThat(v.f2.get("value3").toString(), is("71.03"));
      } else if (v.f2.get("name").toString().equals("bar")) {
        assertThat(v.f2.size(), is(4));
        assertThat(v.f2.get("name").toString(), is("bar"));
        assertThat(v.f2.get("value1").toString(), is("76"));
        assertThat(v.f2.get("value2").toString(), is("false"));
        assertThat(v.f2.get("value3").toString(), is("33.4"));
      } else if (v.f2.get("name").toString().equals("bla")) {
        assertThat(v.f2.size(), is(3));
        assertThat(v.f2.get("name").toString(), is("bla"));
        assertThat(v.f2.get("value1").toString(), is("4568"));
        assertThat(v.f2.get("value3").toString(), is("9.42"));
      } else {
        fail();
      }
    }
  }

  /**
   * Test if the properties of the imported vertices works correct. In this case
   * the file do not contain a header row. For this the user set a list with
   * the column names.
   * @throws Exception
   */
  @Test
  public void testImportWithoutHeader() throws Exception {

    String csvPath = getFilePath("/csv/inputWithoutHeader.csv");
    String delimiter = ";";

    ExecutionEnvironment env = getExecutionEnvironment();

    List<String> columnNames = Arrays.asList("name", "value1", "value2", "value3");
    MinimalCSVImporter importVertexImporter =
            new MinimalCSVImporter(csvPath, delimiter, getConfig(), columnNames);
    DataSet<ImportVertex<Long>> importVertex = importVertexImporter.importVertices(false);

    List<ImportVertex<Long>> lv = new ArrayList<>();
    importVertex.output(new LocalCollectionOutputFormat<>(lv));

    env.execute();

    assertThat(lv.size(), is(3));

    for (ImportVertex<Long> v : lv) {
      if (v.f2.get("name").toString().equals("foo")) {
        assertThat(v.f2.size(), is(4));
        assertThat(v.f2.get("name").toString(), is("foo"));
        assertThat(v.f2.get("value1").toString(), is("453"));
        assertThat(v.f2.get("value2").toString(), is("true"));
        assertThat(v.f2.get("value3").toString(), is("71.03"));
      } else if (v.f2.get("name").toString().equals("bar")) {
        assertThat(v.f2.size(), is(4));
        assertThat(v.f2.get("name").toString(), is("bar"));
        assertThat(v.f2.get("value1").toString(), is("76"));
        assertThat(v.f2.get("value2").toString(), is("false"));
        assertThat(v.f2.get("value3").toString(), is("33.4"));
      } else if (v.f2.get("name").toString().equals("bla")) {
        assertThat(v.f2.size(), is(3));
        assertThat(v.f2.get("name").toString(), is("bla"));
        assertThat(v.f2.get("value1").toString(), is("4568"));
        assertThat(v.f2.get("value3").toString(), is("9.42"));
      } else {
        fail();
      }
    }
  }
}
