/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.EdgeWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;

import java.util.List;

/**
 * Combines the results of a join between ExpandIntermediateResults and an edge embedding by growing
 * the intermediate result.
 * Before growing it is checked whether distinctiveness conditions would still apply.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
@FunctionAnnotation.ForwardedFieldsSecond("f2")
public class MergeExpandEmbeddings
  extends RichFlatJoinFunction<ExpandEmbedding, EdgeWithTiePoint, ExpandEmbedding> {

  /**
   * Holds the index of all vertex columns that should be distinct
   */
  private final List<Integer> distinctVertices;
  /**
   * Holds the index of all edge columns that should be distinct
   */
  private final List<Integer> distinctEdges;
  /**
   * Specifies a base column that should be equal to the paths end node
   */
  private final int closingColumn;

  /**
   * Create a new Combine Expand Embeddings Operator
   * @param distinctVertices distinct vertex columns
   * @param distinctEdges distinct edge columns
   * @param closingColumn base column that should be equal to a paths end node
   */
  public MergeExpandEmbeddings(List<Integer> distinctVertices,
    List<Integer> distinctEdges, int closingColumn) {

    this.distinctVertices = distinctVertices;
    this.distinctEdges = distinctEdges;
    this.closingColumn = closingColumn;
  }

  @Override
  public void join(ExpandEmbedding base, EdgeWithTiePoint edge,
    Collector<ExpandEmbedding> out) throws Exception {

    if (checkDistinctiveness(base, edge)) {
      out.collect(base.grow(edge));
    }
  }

  /**
   * Checks the distinctiveness criteria for the expansion
   * @param prev previous intermediate result
   * @param edge edge along which we expand
   * @return true if distinct criteria apply for the expansion
   */
  private boolean checkDistinctiveness(ExpandEmbedding prev, EdgeWithTiePoint edge) {
    if (distinctVertices.isEmpty() && distinctEdges.isEmpty()) {
      return true;
    }

    // the new candidate is invalid under vertex isomorphism
    if (edge.getSource().equals(edge.getTarget()) &&
      !distinctVertices.isEmpty()) {
      return false;
    }

    // check if there are any clashes in the path
    for (GradoopId ref : prev.getPath()) {
      if ((ref.equals(edge.getSource()) || ref.equals(edge.getTarget()) &&
        !distinctVertices.isEmpty()) || (ref.equals(edge.getId()) && !distinctEdges.isEmpty())) {
        return false;
      }
    }

    List<GradoopId> ref;

    // check for clashes with distinct vertices in the base
    for (int i : distinctVertices) {
      ref = prev.getBase().getIdAsList(i);
      if ((ref.contains(edge.getTarget()) && i != closingColumn) ||
        ref.contains(edge.getSource())) {
        return false;
      }
    }

    // check for clashes with distinct edges in the base
    ref = prev.getBase().getIdsAsList(distinctEdges);
    return !ref.contains(edge.getId());
  }
}
