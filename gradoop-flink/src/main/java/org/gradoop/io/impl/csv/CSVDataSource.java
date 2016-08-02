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

package org.gradoop.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.csv.functions.ImportVertexFromText;
import org.gradoop.io.impl.graph.tuples.ImportVertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

public class CSVDataSource
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends CSVBase<G, V, E>
  implements DataSource<G, V, E> {

  public static final String CACHED_FILE = "cachedfile";

  private String delimiter;


  public CSVDataSource(GradoopFlinkConfig<G, V, E> config, String csvPath,
    String delimiter) {
    super(config, csvPath);
    this.delimiter = delimiter;


  }

  @Override
  public LogicalGraph<G, V, E> getLogicalGraph() throws IOException {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();
    env.registerCachedFile(getCSVPath(), CACHED_FILE);



    DataSet<ImportVertex<String>> vertices = getConfig()
      .getExecutionEnvironment()
      .readTextFile("dfgfd")
      .map(new ImportVertexFromText(delimiter));

    return null;
  }

  @Override
  public GraphCollection<G, V, E> getGraphCollection() throws IOException {
    return null;
  }

  @Override
  public GraphTransactions<G, V, E> getGraphTransactions() throws IOException {
    return null;
  }
}
