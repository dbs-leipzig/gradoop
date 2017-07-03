
package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.ReverseEdgeEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReverseEdgeEmbeddingTest {
  @Test
  public void testReversingAnEdgeEmbedding() throws Exception{
    GradoopId a = GradoopId.get();
    GradoopId e = GradoopId.get();
    GradoopId b = GradoopId.get();

    Embedding edge = new Embedding();
    edge.add(a);
    edge.add(e);
    edge.add(b);

    ReverseEdgeEmbedding op = new ReverseEdgeEmbedding();

    Embedding reversed = op.map(edge);

    assertEquals(b, reversed.getId(0));
    assertEquals(e, reversed.getId(1));
    assertEquals(a, reversed.getId(2));
  }
}
