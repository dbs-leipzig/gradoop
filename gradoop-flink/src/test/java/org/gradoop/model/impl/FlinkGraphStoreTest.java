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

package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import junitparams.JUnitParamsRunner;
import org.gradoop.model.Attributed;
import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.EPGraphData;
import org.gradoop.model.EPGraphElement;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.Labeled;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnitParamsRunner.class)
public class FlinkGraphStoreTest extends EPFlinkTest {

  private EPGraphStore graphStore;

  public FlinkGraphStoreTest() {
    graphStore = createSocialGraph();
  }

  @Test
  public void testGetDatabaseGraph() throws Exception {
    EPGraph dbGraph = graphStore.getDatabaseGraph();

    assertNotNull("database graph was null", dbGraph);
    assertEquals("vertex set has the wrong size", 11,
      dbGraph.getVertices().size());
    assertEquals("edge set has the wrong size", 24, dbGraph.getEdges().size());
  }

  @Test
  public void testFromJsonFile() throws Exception {
    String vertexFile =
      FlinkGraphStoreTest.class.getResource("/sna_nodes").getFile();
    String edgeFile =
      FlinkGraphStoreTest.class.getResource("/sna_edges").getFile();
    String graphFile =
      FlinkGraphStoreTest.class.getResource("/sna_graphs").getFile();

    EPGraphStore graphStore =
      FlinkGraphStore.fromJsonFile(vertexFile, edgeFile, graphFile, env);

    EPGraph databaseGraph = graphStore.getDatabaseGraph();

    assertEquals("Wrong vertex count", 11, databaseGraph.getVertexCount());
    assertEquals("Wrong edge count", 24, databaseGraph.getEdgeCount());
    assertEquals("Wrong graph count", 4,
      graphStore.getCollection().getGraphCount());
  }

  final String curlyOpen = "{";
  final String curlyClose = "}";
  final String bracketOpen = "[";
  final String bracketClose = "]";
  final String id = "\"id\":";
  final String source = "\"source\":";
  final String target = "\"target\":";
  final String meta = "\"meta\":";
  final String data = "\"data\":";
  final String graphs = "\"graphs\":";
  final String label = "\"label\":";
  final String comma = ",";
  final String quote = "\"";
  final String colon = ":";

  @Test
  public void createJson() throws Exception {
    EPGraph graph = graphStore.getDatabaseGraph();

    List<String> lines = Lists.newArrayList();
    StringBuilder sb;
    for (EPVertexData v : graph.getVertices().collect()) {
      sb = new StringBuilder();
      sb.append(curlyOpen);
      sb.append(id);
      sb.append(v.getId());
      sb.append(comma);
      sb = buildData(sb, v);
      sb.append(comma);
      sb = buildMeta(sb, v);
      sb.append(curlyClose);
      lines.add(sb.toString());
    }

    for (EPEdgeData e : graph.getEdges().collect()) {
      sb = new StringBuilder();
      sb.append(curlyOpen);
      sb.append(id);
      sb.append(e.getId());
      sb.append(comma);
      sb.append(source);
      sb.append(e.getSourceVertex());
      sb.append(comma);
      sb.append(target);
      sb.append(e.getTargetVertex());
      sb.append(comma);
      sb = buildData(sb, e);
      sb.append(comma);
      sb = buildMeta(sb, e);
      sb.append(curlyClose);
      lines.add(sb.toString());
    }

    for (EPGraphData g : graphStore.getCollection().collect()) {
      sb = new StringBuilder();
      sb.append(curlyOpen);
      sb.append(id);
      sb.append(g.getId());
      sb.append(comma);
      sb = buildData(sb, g);
      sb.append(comma);
      sb.append(meta);
      sb.append(curlyOpen);
      sb.append(label);
      sb.append(quote).append(g.getLabel()).append(quote);
      sb.append(curlyClose);
      sb.append(curlyClose);
      lines.add(sb.toString());
    }

    for (String line : lines) {
      System.out.println(line);
    }

  }

  private StringBuilder buildData(StringBuilder sb, Attributed entity) {
    sb.append(data);
    sb.append(curlyOpen);
    int propCount = 0;
    for (Map.Entry<String, Object> p : entity.getProperties().entrySet()) {
      sb.append(quote).append(p.getKey()).append(quote).append(colon);
      sb.append((p.getValue() instanceof Number) ? p.getValue() :
        (quote + p.getValue() + quote));
      sb.append(comma);
      propCount++;
    }
    if (propCount > 0) {
      sb.delete(sb.length() - 1, sb.length());
    }
    sb.append(curlyClose);
    return sb;
  }

  <T extends Labeled & EPGraphElement> StringBuilder buildMeta(StringBuilder sb,
    T entity) {
    sb.append(meta);
    sb.append(curlyOpen);
    sb.append(label);
    sb.append(quote).append(entity.getLabel()).append(quote);
    if (entity.getGraphs().size() > 0) {
      sb.append(comma);
      sb.append(graphs);
      sb.append(bracketOpen);
      for (Long graphID : entity.getGraphs()) {
        sb.append(graphID);
        sb.append(comma);
      }
      sb.delete(sb.length() - 1, sb.length());
      sb.append(bracketClose);
    }
    sb.append(curlyClose);
    return sb;
  }

}
