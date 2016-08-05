/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.csv;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

public class CSVDataSink extends CSVBase implements DataSink {

  public CSVDataSink(GradoopFlinkConfig config, String metaXmlPath) {
    super(config, metaXmlPath);
  }

  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {

  }

  @Override
  public void write(GraphCollection graphCollection) throws
    IOException {

  }

  @Override
  public void write(GraphTransactions graphTransactions) throws
    IOException {

  }
}
