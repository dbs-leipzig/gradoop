package org.gradoop.biiig.algorithms;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.GiraphClusterBasedTest;
import org.gradoop.io.reader.AdjacencyListReader;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.inmemory.MemoryEdge;
import org.gradoop.model.inmemory.MemoryVertex;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Tests for {@link BTGComputation} that reads BTGs from HBase.
 */
public class BTGHBaseComputationTest extends GiraphClusterBasedTest {
  public BTGHBaseComputationTest() {
    super(BTGHBaseComputationTest.class.getName());
  }

  @Test
  public void testConnectedIIG()
    throws IOException {
    BufferedReader inputReader = createTestReader(BTGComputationTestHelper
      .getConnectedIIG());
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader = new AdjacencyListReader
      (graphStore, new BTGLineReader());
    adjacencyListReader.read(inputReader);

    graphStore.close();

  }

  private static class BTGLineReader implements VertexLineReader {
    final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(",");
    final Pattern VALUE_TOKEN_SEPARATOR = Pattern.compile(" ");

    @Override
    public Vertex readLine(String line) {
      String[] lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      Long vertexID = Long.valueOf(lineTokens[0]);
      String[] valueTokens = VALUE_TOKEN_SEPARATOR.split(lineTokens[1]);
      // read vertex type as label
      String vertexType = valueTokens[0];
      // read vertex value
      Integer vertexValue = Integer.valueOf(valueTokens[1]);
      Map<String, Object> props = Maps.newHashMapWithExpectedSize(1);
      props.put("val", vertexValue);
      // edges
      String[] stringEdges = VALUE_TOKEN_SEPARATOR.split(lineTokens[2]);
      List<Edge> edges = Lists.newArrayListWithCapacity(stringEdges.length);
      for (String edge : stringEdges) {
        Long otherID = Long.valueOf(edge);
        edges.add(new MemoryEdge(otherID, 0L));
      }
      return new MemoryVertex(vertexID, Lists.newArrayList(vertexType),
        props, edges, null, null);
    }
  }
}
