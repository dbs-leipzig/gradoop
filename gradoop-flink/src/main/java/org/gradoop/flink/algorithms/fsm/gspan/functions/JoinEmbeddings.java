package org.gradoop.flink.algorithms.fsm.gspan.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.AdjacencyMatrix;
import org.gradoop.flink.algorithms.fsm.CodeEmbeddings;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by peet on 09.09.16.
 */
public class JoinEmbeddings
  implements GroupReduceFunction<CodeEmbeddings, CodeEmbeddings> {

  private final CodeEmbeddings reuseTuple = new CodeEmbeddings();

  private final int fromSize;
  private final int toSize;

  public JoinEmbeddings(int fromSize, int toSize) {
    this.fromSize = fromSize;
    this.toSize = toSize;
  }

  @Override
  public void reduce(
    Iterable <CodeEmbeddings> values, Collector <CodeEmbeddings> out
  ) throws Exception {

    Collection<CodeEmbeddings> cachedValues = Lists.newArrayList();

    if (toSize == 1) {
      
      for (CodeEmbeddings value : values) {

        cachedValues.add(value);

        for (CodeEmbeddings cachedValue : cachedValues) {
          joinOnVertex(cachedValue, value);
        }
      }

    } else {

    }
  }

  private void joinOnVertex(CodeEmbeddings left, CodeEmbeddings right) {

    String leftSubgraph = left.getSubgraph();
    String rightSubgraph = right.getSubgraph();

    for (AdjacencyMatrix leftMatrix : left.getMatrices()) {
      for (AdjacencyMatrix rightMatrix : right.getMatrices()) {

        Set<Integer> commonVertexIds =
          leftMatrix.getVertices().keySet();

        Set<Integer> rightVertexIds =
          Sets.newHashSet(leftMatrix.getVertices().keySet());

        Iterator<Integer> vertexIterator = commonVertexIds.iterator();

        while (vertexIterator.hasNext()) {
          if (! rightVertexIds.contains(vertexIterator.next())) {
            vertexIterator.remove();
          }
        }

        if (! commonVertexIds.isEmpty() && Collections.disjoint(
          leftMatrix.getEdges().keySet(), rightMatrix.getEdges().keySet())) {

          leftMatrix.combine(rightMatrix);
        }
      }
    }
  }

}
