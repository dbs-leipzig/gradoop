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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.grouping;


import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.MaxAggregator;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public abstract class EdgeCentricGroupingTestBase extends GradoopFlinkTestBase {

  public abstract GroupingStrategy getStrategy();

  @Test
  public void testRead() throws Exception {
    String graphFile =
      EdgeCentricGroupingTestBase.class.getResource("/data/grouping/graphs.json").getFile();
    String vertexFile =
      EdgeCentricGroupingTestBase.class.getResource("/data/grouping/nodes.json").getFile();
    String edgeFile =
      EdgeCentricGroupingTestBase.class.getResource("/data/grouping/edges.json").getFile();

    DataSource dataSource = new JSONDataSource(graphFile, vertexFile, edgeFile, config);


    LogicalGraph output = new Grouping.GroupingBuilder()
//      .addEdgeGroupingKey(Grouping.LABEL_SYMBOL)
//      .addEdgeGroupingKeys(Arrays.asList(Grouping.LABEL_SYMBOL, Grouping.SOURCE_SYMBOL, Grouping.TARGET_SYMBOL))
      .addEdgeGroupingKeys(Arrays.asList(Grouping.LABEL_SYMBOL, Grouping.SOURCE_SYMBOL))
//      .addEdgeGroupingKeys(Arrays.asList(Grouping.LABEL_SYMBOL,Grouping.TARGET_SYMBOL))
      .addEdgeAggregator(new CountAggregator("edgeCount"))
      .addVertexGroupingKey(Grouping.LABEL_SYMBOL)
      .addVertexAggregator(new CountAggregator("vertexCount"))
      .addVertexAggregator(new MaxAggregator("vId", "max"))
      .setStrategy(getStrategy())
      .setCentricalStrategy(GroupingStrategy.EDGE_CENTRIC)
      .build()
      .execute(dataSource.getLogicalGraph());

    Collection<GraphHead> graphHeads = Lists.newArrayList();
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();

    output.getGraphHead().output(new LocalCollectionOutputFormat<>(graphHeads));
    output.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    output.getEdges().output(new LocalCollectionOutputFormat<>(edges));

    getExecutionEnvironment().execute();
    for (GraphHead graphHead : graphHeads) {
      System.out.println(graphHead);
    }
    for (Vertex vertex : vertices) {
      System.out.println(vertex);
    }
    for (Edge edge : edges) {
      System.out.println(edge);
    }

    assertEquals("Wrong graph count", 1, graphHeads.size());
    assertEquals("Wrong vertex count", 5, vertices.size());
    assertEquals("Wrong edge count", 3, edges.size());
  }
}