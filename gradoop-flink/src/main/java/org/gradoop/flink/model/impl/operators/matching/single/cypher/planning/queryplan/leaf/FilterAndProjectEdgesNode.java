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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.FilterAndProjectEdges;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.ProjectionNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Leaf node that wraps a {@link FilterAndProjectEdges} operator.
 */
public class FilterAndProjectEdgesNode extends LeafNode implements FilterNode, ProjectionNode {
  /**
   * Input data set
   */
  private DataSet<Edge> edges;
  /**
   * Query variable of the source vertex
   */
  private final String sourceVariable;
  /**
   * Query variable of the edge
   */
  private final String edgeVariable;
  /**
   * Query variable of the target vertex
   */
  private final String targetVariable;
  /**
   * Filter predicate that is applied on the input data set
   */
  private CNF filterPredicate;
  /**
   * Property keys used for projection
   */
  private final List<String> projectionKeys;
  /**
   * Indicates if the edges is actually a path
   */
  private final boolean isPath;

  /**
   * Creates a new node.
   *
   * @param edges input edges
   * @param sourceVariable query variable of the source vertex
   * @param edgeVariable query variable of the edge
   * @param targetVariable query variable of the target vertex
   * @param filterPredicate filter predicate to be applied on edges
   * @param projectionKeys property keys whose associated values are projected to the output
   * @param isPath indicates if the edges is actually a path
   */
  public FilterAndProjectEdgesNode(DataSet<Edge> edges,
    String sourceVariable, String edgeVariable, String targetVariable,
    CNF filterPredicate, Set<String> projectionKeys, boolean isPath) {
    this.edges = edges;
    this.sourceVariable = sourceVariable;
    this.edgeVariable = edgeVariable;
    this.targetVariable = targetVariable;
    this.filterPredicate = filterPredicate;
    this.projectionKeys = new ArrayList<>(projectionKeys);
    this.isPath = isPath;
  }

  @Override
  public DataSet<Embedding> execute() {
    FilterAndProjectEdges op =  new FilterAndProjectEdges(
      edges,
      filterPredicate,
      projectionKeys,
      isLoop()
    );
    op.setName(toString());
    return op.evaluate();
  }

  /**
   * Returns a copy of the filter predicate attached to this node.
   *
   * @return filter predicate
   */
  public CNF getFilterPredicate() {
    return new CNF(filterPredicate);
  }

  /**
   * Returns a copy of the projection keys attached to this node.
   *
   * @return projection keys
   */
  public List<String> getProjectionKeys() {
    return new ArrayList<>(projectionKeys);
  }

  public boolean isLoop() {
    return sourceVariable.equals(targetVariable) && !isPath;
  }

  @Override
  protected EmbeddingMetaData computeEmbeddingMetaData() {
    EmbeddingMetaData embeddingMetaData = new EmbeddingMetaData();
    embeddingMetaData.setEntryColumn(sourceVariable, EmbeddingMetaData.EntryType.VERTEX, 0);
    embeddingMetaData.setEntryColumn(edgeVariable, EmbeddingMetaData.EntryType.EDGE, 1);
    if (!isLoop()) {
      embeddingMetaData.setEntryColumn(targetVariable, EmbeddingMetaData.EntryType.VERTEX, 2);
    }

    embeddingMetaData = setPropertyColumns(embeddingMetaData, edgeVariable, projectionKeys);

    return embeddingMetaData;
  }

  @Override
  public String toString() {
    return String.format("FilterAndProjectEdgesNode{" +
        "sourceVariable='%s', " +
        "edgeVariable='%s', " +
        "targetVariable='%s', " +
        "filterPredicate=%s, " +
        "projectionKeys=%s}",
      sourceVariable, edgeVariable, targetVariable, filterPredicate, projectionKeys);
  }
}
