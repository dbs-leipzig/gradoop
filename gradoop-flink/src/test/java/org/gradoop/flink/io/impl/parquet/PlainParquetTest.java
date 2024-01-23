/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.parquet;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.parquet.plain.ParquetDataSink;
import org.gradoop.flink.io.impl.parquet.plain.ParquetDataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PlainParquetTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testPlainParquet() throws Exception {
    try {
      ExecutionEnvironment executionEnvironment = getExecutionEnvironment();

      GraphCollection input = getSocialNetworkLoader().getGraphCollection();
      String tmpPath = temporaryFolder.getRoot().getPath();

      DataSink dataSink = new ParquetDataSink(tmpPath, getConfig());
      dataSink.write(input, false);

      executionEnvironment.execute();

      DataSource dataSource = new ParquetDataSource(tmpPath, getConfig());
      GraphCollection output = dataSource.getGraphCollection();

      collectAndAssertTrue(input.equalsByGraphElementData(output));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
