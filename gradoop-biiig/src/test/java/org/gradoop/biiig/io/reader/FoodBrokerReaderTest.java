package org.gradoop.biiig.io.reader;

import com.google.common.collect.Lists;
import org.gradoop.HBaseClusterTest;
import org.gradoop.io.reader.VertexListLineReader;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link org.gradoop.biiig.io.reader.FoodBrokerReader}.
 */
public class FoodBrokerReaderTest extends HBaseClusterTest {

  private static final int FOODBROKER_SAMPLE_VCOUNT = 6;
  private static final int FOODBROKER_SAMPLE_ECOUNT = 5;

  private static final String[] FOODBROKER_SAMPLE = new String[] {
    // nodes
    "{\"id\":1,\"data\":{\"num\":\"EMP0000001\",\"name\":\"Trace Armstrong\"," +
      "\"gender\":\"male\"},\"meta\":{\"system\":\"ERP\"," +
      "\"quality\":\"good\",\"class\":\"Employee\",\"kind\":\"MasterData\"}}",

    "{\"id\":2,\"data\":{\"num\":\"EMP0000002\",\"name\":\"Sonya Barrett\"," +
      "\"gender\":\"female\"},\"meta\":{\"system\":\"ERP\"," +
      "\"quality\":\"good\",\"class\":\"Employee\",\"kind\":\"MasterData\"}}",

    "{\"id\":3,\"data\":{\"num\":\"EMP0000003\",\"name\":\"Ruben Bernard\"," +
      "\"gender\":\"male\"},\"meta\":{\"system\":\"ERP\"," +
      "\"quality\":\"good\",\"class\":\"Employee\",\"kind\":\"MasterData\"}}",

    "{\"id\":41,\"data\":{\"num\":\"PRD0000041\",\"category\":\"nuts\"," +
      "\"price\":2.28,\"name\":\"dark sweet Acorn\"}," +
      "\"meta\":{\"system\":\"ERP\",\"quality\":\"good\"," +
      "\"class\":\"Product\",\"kind\":\"MasterData\"}}",

    "{\"id\":42,\"data\":{\"num\":\"PRD0000042\",\"category\":\"nuts\"," +
      "\"price\":9.4,\"name\":\"original short Acorn\"}," +
      "\"meta\":{\"system\":\"ERP\",\"quality\":\"good\"," +
      "\"class\":\"Product\",\"kind\":\"MasterData\"}}",

    "{\"id\":43,\"data\":{\"num\":\"PRD0000043\",\"category\":\"nuts\"," +
      "\"price\":2.06,\"name\":\"short original Acorn\"}," +
      "\"meta\":{\"system\":\"ERP\",\"quality\":\"good\"," +
      "\"class\":\"Product\",\"kind\":\"MasterData\"}}",
    // edges
    "{\"start\":1,\"data\":{},\"meta\":{\"type\":\"sentBy\"},\"end\":2}",

    "{\"start\":1,\"data\":{},\"meta\":{\"type\":\"sentTo\"},\"end\":3}",

    "{\"start\":2,\"data\":{\"salesPrice\":8.44,\"purchPrice\":8.04," +
      "\"quantity\":25},\"meta\":{\"system\":\"ERP\"," +
      "\"type\":\"SalesQuotationLine\"},\"end\":41}",

    "{\"start\":41,\"data\":{\"salesPrice\":7.22,\"purchPrice\":6.88," +
      "\"quantity\":85},\"meta\":{\"system\":\"ERP\"," +
      "\"type\":\"SalesQuotationLine\"},\"end\":42}",

    "{\"start\":41,\"data\":{\"salesPrice\":6.74,\"purchPrice\":6.54," +
      "\"quantity\":85},\"meta\":{\"system\":\"ERP\"," +
      "\"type\":\"SalesQuotationLine\"},\"end\":42}"
  };

  @Test
  public void readFromJsonTest()
    throws IOException {
    FoodBrokerReader reader = new FoodBrokerReader();
    List<Vertex> vertices = Lists.newArrayList();
    for (String line : FOODBROKER_SAMPLE) {
      vertices.addAll(reader.readLine(line));
    }
    // reader creates a vertex for each line in the input: 1 vertex for each
    // vertex line and 2 vertices for each edge line (outgoing + incoming)
    assertEquals(FOODBROKER_SAMPLE_VCOUNT + 2 * FOODBROKER_SAMPLE_ECOUNT,
      vertices.size());
  }

  @Test
  public void loadJsonToHBaseTest()
    throws IOException {
    GraphStore graphStore = createEmptyGraphStore();
    VertexListLineReader foodbrokerReader = new FoodBrokerReader();
    for (String line : FOODBROKER_SAMPLE) {
      for (Vertex v : foodbrokerReader.readLine(line)) {
        graphStore.writeVertex(v);
      }
    }
    validateFoodbrokerGraph(graphStore);
    graphStore.close();
  }

  private void validateFoodbrokerGraph(GraphStore graphStore) {
    validateVertex(graphStore.readVertex(1L), 7, 1, 2, 0);
    validateVertex(graphStore.readVertex(2L), 7, 1, 1, 1);
    validateVertex(graphStore.readVertex(3L), 7, 1, 0, 1);
    validateVertex(graphStore.readVertex(41L), 8, 1, 2, 1);
    validateVertex(graphStore.readVertex(42L), 8, 1, 0, 2);
    validateVertex(graphStore.readVertex(43L), 8, 1, 0, 0);
  }

  private void validateVertex(Vertex vertex, int expectedPropertyCount, int
    expectedLabelCount, int expectedOutEdgesCount, int expectedInEdgesCount) {
    assertEquals(expectedPropertyCount, vertex.getPropertyCount());
    assertEquals(expectedLabelCount, vertex.getLabelCount());
    List<Edge> outgoingEdges = Lists.newArrayList(vertex.getOutgoingEdges());
    assertEquals(expectedOutEdgesCount, outgoingEdges.size());
    List<Edge> incomingEdges = Lists.newArrayList(vertex.getIncomingEdges());
    assertEquals(expectedInEdgesCount, incomingEdges.size());
  }
}
