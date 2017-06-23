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

package org.gradoop.examples.grouping;

import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.labelpropagation.GellyLabelPropagation;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static java.util.Collections.singletonList;

/**
 * Demo program that uses {@link GellyLabelPropagation} to compute communities in a social network
 * and groups the vertices by their community identifier. The result is a summary graph where each
 * vertex represents a community including its user count while each edge represents all friendships
 * between communities.
 */
public class Communities extends AbstractRunner {

  /**
   * Loads a social network graph from the specified location, applies label propagation to extract
   * communities and computes a summary graph using the community id. The resulting summary graph is
   * written using the DOT format.
   *
   * args[0] - input path (CSV)
   * args[1] - output path
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];

    // instantiate a default gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());

    // define a data source to load the graph
    DataSource dataSource = new CSVDataSource(inputPath, config);

    // property key used for label propagation
    final String communityKey = "c_id";

    // load the graph and set initial community id
    LogicalGraph graph = dataSource.getLogicalGraph();
    graph = graph.transformVertices((current, transformed) -> {
        current.setProperty(communityKey, current.getId());
        return current;
      });

    // apply label propagation to compute communities
    graph = graph.callForGraph(new GellyLabelPropagation(10, communityKey));

    // group the vertices of the graph by their community and count the edges between communities
    LogicalGraph communities = graph.groupBy(singletonList(communityKey));

    // instantiate a data sink for the DOT format
    DataSink dataSink = new CSVDataSink(outputPath, config);
    dataSink.write(communities, true);

    // run the job
    getExecutionEnvironment().execute();
  }
}
