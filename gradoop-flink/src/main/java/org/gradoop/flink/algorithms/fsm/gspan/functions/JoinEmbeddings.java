package org.gradoop.flink.algorithms.fsm.gspan.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.CodeEmbeddings;
import org.gradoop.flink.algorithms.fsm.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSEmbedding;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSStep;

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

    CompressedDFSCode leftSubgraph = left.getSubgraph();
    CompressedDFSCode rightSubgraph = right.getSubgraph();

    for (DFSEmbedding leftEmbedding : left.getEmbeddings()) {
      for (DFSEmbedding rightEmbedding : right.getEmbeddings()) {

        Set<Integer> commonVertexIds =
          Sets.newHashSet(leftEmbedding.getVertexTimes());

        Set<Integer> rightVertexIds =
          Sets.newHashSet(leftEmbedding.getVertexTimes());

        Iterator<Integer> vertexIterator = commonVertexIds.iterator();

        while (vertexIterator.hasNext()) {
          if (! rightVertexIds.contains(vertexIterator.next())) {
            vertexIterator.remove();
          }
        }

        if (! commonVertexIds.isEmpty() && Collections.disjoint(
          leftEmbedding.getEdgeTimes(), leftEmbedding.getEdgeTimes())) {

          joinOn(leftSubgraph, rightSubgraph, leftEmbedding, rightEmbedding,
            commonVertexIds);
        }
      }
    }
  }

  private void joinOn(
    CompressedDFSCode leftSubgraph, CompressedDFSCode rightSubgraph,
    DFSEmbedding leftEmbedding, DFSEmbedding rightEmbedding,
    Set<Integer> commonVertexIds) {

    Iterator<DFSStep> leftIterator =
      leftSubgraph.getDfsCode().getSteps().iterator();

    Iterator<DFSStep> rightIterator =
      rightSubgraph.getDfsCode().getSteps().iterator();


  }
}
