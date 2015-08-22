package org.gradoop.model.impl.operators.summarization;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.KeySelectors;

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
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class SummarizationGroupSort<VD extends VertexData, ED extends
  EdgeData, GD extends GraphData> extends
  Summarization<VD, ED, GD> {

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
    /* build summarized vertices */
    SortedGrouping<Vertex<Long, VD>> groupedSortedVertices =
      groupVertices(graph)
        .sortGroup(new KeySelectors.VertexKeySelector<VD>(), Order.ASCENDING);
    // create new summarized gelly vertices
    DataSet<Vertex<Long, VD>> newVertices =
      buildSummarizedVertices(groupedSortedVertices);

    // create mapping from vertex-id to group representative
    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      groupedSortedVertices
        .reduceGroup(new VertexToRepresentativeReducer<VD>());

    /* build summarized vertices */
    DataSet<Edge<Long, ED>> newEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return Graph.fromDataSet(newVertices, newEdges, graph.getContext());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SummarizationGroupSort.class.getName();
  }

  /**
   * Takes a group of vertex ids as input an emits a (vertex-id,
   * group-representative) tuple for each vertex in that group.
   * <p/>
   * The group representative is the first vertex-id in the group.
   */
  private static class VertexToRepresentativeReducer<VD extends VertexData>
    implements
    GroupReduceFunction<Vertex<Long, VD>, VertexWithRepresentative> {

    /**
     * Avoid object instantiation in each reduce call.
     */
    private final VertexWithRepresentative reuseTuple;

    /**
     * Creates a group reduce function.
     */
    public VertexToRepresentativeReducer() {
      this.reuseTuple = new VertexWithRepresentative();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<Vertex<Long, VD>> group,
      Collector<VertexWithRepresentative> collector) throws Exception {
      Long groupRepresentative = null;
      boolean first = true;
      for (Vertex<Long, VD> groupElement : group) {
        if (first) {
          groupRepresentative = groupElement.getId();
          first = false;
        }
        reuseTuple.setVertexId(groupElement.getId());
        reuseTuple.setGroupRepresentativeVertexId(groupRepresentative);
        collector.collect(reuseTuple);
      }
    }
  }
}
