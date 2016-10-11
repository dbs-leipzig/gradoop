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

package org.gradoop.flink.io.impl.json;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class JSONDataSourceTest extends GradoopFlinkTestBase {

  @Test
  public void testRead() throws Exception {
    String vertexFile =
      JSONDataSourceTest.class.getResource("/data/json/sna/nodes.json").getFile();
    String edgeFile =
      JSONDataSourceTest.class.getResource("/data/json/sna/edges.json").getFile();
    String graphFile =
      JSONDataSourceTest.class.getResource("/data/json/sna/graphs.json").getFile();

    DataSource dataSource = new JSONDataSource(
      graphFile, vertexFile, edgeFile, config);

    GraphCollection
      collection = dataSource.getGraphCollection();

    Collection<GraphHead> graphHeads = Lists.newArrayList();
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();

    collection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(graphHeads));
    collection.getVertices()
      .output(new LocalCollectionOutputFormat<>(vertices));
    collection.getEdges()
      .output(new LocalCollectionOutputFormat<>(edges));

    getExecutionEnvironment().execute();

    assertEquals("Wrong graph count", 4, graphHeads.size());
    assertEquals("Wrong vertex count", 11, vertices.size());
    assertEquals("Wrong edge count", 24, edges.size());
  }
}
