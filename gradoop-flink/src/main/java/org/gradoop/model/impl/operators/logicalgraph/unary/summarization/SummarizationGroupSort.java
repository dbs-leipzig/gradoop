package org.gradoop.model.impl.operators.logicalgraph.unary.summarization;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.functions.VertexToGroupVertexMapper;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.functions
  .VertexGroupItemToRepresentativeFilter;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.functions
  .VertexGroupItemToSummarizedVertexFilter;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.functions
  .VertexGroupItemToSummarizedVertexMapper;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.functions
  .VertexGroupItemToVertexWithRepresentativeMapper;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.functions
  .VertexGroupReducer;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.tuples.VertexForGrouping;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.tuples
  .VertexWithRepresentative;

/**
 * Summarization implementation that requires sorting of vertex groups to chose
 * a group representative.
 *
 * Algorithmic idea:
 *
 * 1) group vertices by label / property / both
 * 2) sort groups by vertex identifier ascending
 * 3a) reduce group 1
 * - build summarized vertex from each group (group count, group label/prop)
 * 3b) reduce group 2
 * - build {@link VertexWithRepresentative} tuples for each group element
 * 4) join output from 3b) with edges
 * - replace source / target vertex id with vertex group representative
 * 5) group edges on source/target vertex and possibly edge label / property
 * 6) build summarized edges
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class SummarizationGroupSort<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  extends Summarization<VD, ED, GD> {

  /**
   * Creates summarization.
   *
   * @param vertexGroupingKey property key to summarize vertices
   * @param edgeGroupingKey   property key to summarize edges
   * @param useVertexLabels   summarize on vertex label true/false
   * @param useEdgeLabels     summarize on edge label true/false
   */
  public SummarizationGroupSort(String vertexGroupingKey,
    String edgeGroupingKey, boolean useVertexLabels, boolean useEdgeLabels) {
    super(vertexGroupingKey, edgeGroupingKey, useVertexLabels, useEdgeLabels);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Graph<Long, VD, ED> summarizeInternal(Graph<Long, VD, ED> graph) {
    DataSet<VertexForGrouping> verticesForGrouping = graph.getVertices()
      // map vertices to a compact representation
      .map(new VertexToGroupVertexMapper<VD>(
        getVertexGroupingKey(), useVertexLabels()));

    // sort group by vertex id ascending
    DataSet<VertexGroupItem> sortedGroupedVertices =
      // group vertices by label / property / both
      groupVertices(verticesForGrouping)
        // sort group by vertex id ascending
        .sortGroup(0, Order.ASCENDING)
        // create vertex group items
        .reduceGroup(new VertexGroupReducer());

    DataSet<Vertex<Long, VD>> summarizedVertices = sortedGroupedVertices
      // filter group representative tuples
      .filter(new VertexGroupItemToSummarizedVertexFilter())
        // build summarized vertex
      .map(new VertexGroupItemToSummarizedVertexMapper<>(
        config.getVertexFactory(), getVertexGroupingKey(), useVertexLabels()));

    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      sortedGroupedVertices
        // filter group element tuples
        .filter(new VertexGroupItemToRepresentativeFilter())
          // build vertex to group representative tuple
        .map(new VertexGroupItemToVertexWithRepresentativeMapper());

    // build summarized edges
    DataSet<Edge<Long, ED>> summarizedEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return Graph
      .fromDataSet(summarizedVertices, summarizedEdges, graph.getContext());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SummarizationGroupSort.class.getName();
  }
}
