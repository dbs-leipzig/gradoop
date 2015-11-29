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

package org.gradoop.model.impl.operators.summarization;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.summarization.functions.VertexToGroupVertexMapper;
import org.gradoop.model.impl.operators.summarization.functions
  .VertexGroupItemToRepresentativeFilter;
import org.gradoop.model.impl.operators.summarization.functions
  .VertexGroupItemToSummarizedVertexFilter;
import org.gradoop.model.impl.operators.summarization.functions
  .VertexGroupItemToSummarizedVertexMapper;
import org.gradoop.model.impl.operators.summarization.functions
  .VertexGroupItemToVertexWithRepresentativeMapper;
import org.gradoop.model.impl.operators.summarization.tuples.VertexForGrouping;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.summarization.tuples
  .VertexWithRepresentative;

/**
 * Summarization implementation that does not require sorting of vertex groups.
 *
 * Note: This implementation uses the group combine operator in Flink. The
 * operator is performed in memory and throws exceptions when the data to
 * process exceed available memory.
 *
 * Algorithmic idea:
 *
 * 1) group vertices by label / property / both
 * 2) combine groups:
 * - convert vertex data to smaller representation {@link VertexGroupItem}
 * - create partition representative tuple (partition count, group label/prop)
 * 3) group combine output by label / property / both
 * 4) reduce groups
 * - pick group representative from all partitions
 * - update input tuples with group representative and collect them
 * - create group representative tuple (group count, group label/prop)
 * 5) filter group representative tuples from group reduce output
 * - build summarized vertices
 * 6) filter non group representative tuples from group reduce output
 * - build {@link VertexWithRepresentative} tuples
 * 7) join output from 6) with edges
 * - replace source / target vertex id with vertex group representative
 * 8) group edges on source/target vertex and possibly edge label / property
 * 9) build summarized edges
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class SummarizationGroupCombine<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends Summarization<G, V, E> {
  /**
   * Creates summarization.
   *
   * @param vertexGroupingKey property key to summarize vertices
   * @param edgeGroupingKey   property key to summarize edges
   * @param useVertexLabels   summarize on vertex label true/false
   * @param useEdgeLabels     summarize on edge label true/false
   */
  public SummarizationGroupCombine(String vertexGroupingKey,
    String edgeGroupingKey, boolean useVertexLabels, boolean useEdgeLabels) {
    super(vertexGroupingKey, edgeGroupingKey, useVertexLabels, useEdgeLabels);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Graph<GradoopId, V, E> summarizeInternal(
    Graph<GradoopId, V, E> graph) {

    /* build summarized vertices */
    // map vertex data to a smaller representation for grouping
    DataSet<VertexForGrouping> verticesForGrouping = graph.getVertices().map(
      new VertexToGroupVertexMapper<V>(getVertexGroupingKey(),
        useVertexLabels()));

    // group vertices by either label or property or both
    UnsortedGrouping<VertexForGrouping> groupedVertices =
      groupVertices(verticesForGrouping);

    // combine groups on partition basis
    DataSet<VertexGroupItem> combineGroup =
      groupedVertices.combineGroup(new VertexGroupCombineFunction());

    // group output again
    UnsortedGrouping<VertexGroupItem> unsortedGrouping =
      groupVertexGroupItems(combineGroup);

    // group reduce groups
    GroupReduceOperator<VertexGroupItem, VertexGroupItem> reduceGroup =
      unsortedGrouping.reduceGroup(new VertexGroupReduceFunction());

    // filter group representative tuples and build final vertices
    DataSet<Vertex<GradoopId, V>> summarizedVertices =
      reduceGroup.filter(new VertexGroupItemToSummarizedVertexFilter()).map(
        new VertexGroupItemToSummarizedVertexMapper<>(config.getVertexFactory(),
          getVertexGroupingKey(), useVertexLabels()));

    // filter vertex to representative tuples (used for edge join)
    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      reduceGroup.filter(new VertexGroupItemToRepresentativeFilter())
        .map(new VertexGroupItemToVertexWithRepresentativeMapper());

    // build summarized edges
    DataSet<Edge<GradoopId, E>> summarizedEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return Graph
      .fromDataSet(summarizedVertices, summarizedEdges, graph.getContext());
  }

  /**
   * Takes a vertex as input and creates an initial {@link VertexGroupItem}.
   * As group combine is executed per partition, there may be partial results
   * which need to be combined in the group reduce function later.
   *
   * Each partition creates a {@link VertexGroupItem} for each element in the
   * group and one additional {@link VertexGroupItem} that represents the
   * partition (i.e, partition count).
   */
  private static class VertexGroupCombineFunction implements
    GroupCombineFunction<VertexForGrouping, VertexGroupItem> {

    /**
     * Avoid additional object initialization.
     */
    private final VertexGroupItem reuseVertexGroupItem;

    /**
     * Creates group combine function
     */
    public VertexGroupCombineFunction() {
      this.reuseVertexGroupItem = new VertexGroupItem();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void combine(Iterable<VertexForGrouping> vertices,
      Collector<VertexGroupItem> collector) throws Exception {

      GradoopId groupRepresentativeVertexId = null;
      Long groupPartitionCount = 0L;
      boolean firstElement = true;

      for (VertexForGrouping vertex : vertices) {
        reuseVertexGroupItem.setVertexId(vertex.getVertexId());
        if (firstElement) {
          groupRepresentativeVertexId = vertex.getVertexId();
          reuseVertexGroupItem
            .setGroupRepresentativeVertexId(groupRepresentativeVertexId);
          reuseVertexGroupItem.setGroupLabel(vertex.getGroupLabel());
          reuseVertexGroupItem
            .setGroupPropertyValue(vertex.getGroupPropertyValue());

          firstElement = false;
        }

        groupPartitionCount++;
        collector.collect(reuseVertexGroupItem);
      }
      reuseVertexGroupItem.setVertexId(groupRepresentativeVertexId);
      reuseVertexGroupItem.setGroupCount(groupPartitionCount);

      collector.collect(reuseVertexGroupItem);
      reuseVertexGroupItem.reset();
    }
  }


  /**
   * Takes the output tuples of the group combine step and chooses a group
   * representative (group representative of first incoming tuple).
   * Replaces the group representative of all following tuples with group
   * final group representative.
   * <p/>
   * When reading partition representative tuple, the final group count gets
   * accumulated and the group label / property is read from the first
   * partition representative tuple.
   */
  private static class VertexGroupReduceFunction implements
    GroupReduceFunction<VertexGroupItem, VertexGroupItem> {

    /**
     * Avoid object instantiation.
     */
    private final VertexGroupItem reuseVertexGroupItem;

    /**
     * Create group reduce function.
     */
    private VertexGroupReduceFunction() {
      reuseVertexGroupItem = new VertexGroupItem();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<VertexGroupItem> vertexGroupItems,
      Collector<VertexGroupItem> collector) throws Exception {
      GradoopId groupRepresentativeVertexId = null;
      Long groupCount = 0L;
      String groupLabel = null;
      String groupPropertyValue = null;
      boolean firstElement = true;

      for (VertexGroupItem vertexGroupItem : vertexGroupItems) {
        if (firstElement) {
          // take final group representative vertex id from first tuple
          groupRepresentativeVertexId =
            vertexGroupItem.getGroupRepresentativeVertexId();
          // if label grouping is needed, each tuple carries the label
          if (vertexGroupItem.getGroupLabel() != null) {
            groupLabel = vertexGroupItem.getGroupLabel();
          }
          // if property grouping is needed, each tuple carries the value
          if (vertexGroupItem.getGroupPropertyValue() != null) {
            groupPropertyValue = vertexGroupItem.getGroupPropertyValue();
          }
          firstElement = false;
        }
        reuseVertexGroupItem.setVertexId(vertexGroupItem.getVertexId());
        reuseVertexGroupItem
          .setGroupRepresentativeVertexId(groupRepresentativeVertexId);

        // only partition representative tuples have a group count > 0
        if (vertexGroupItem.getGroupCount().equals(0L)) {
          collector.collect(reuseVertexGroupItem);
        } else {
          // accumulate final group count from partition representative tuples
          groupCount += vertexGroupItem.getGroupCount();
        }
      }
      reuseVertexGroupItem.setVertexId(groupRepresentativeVertexId);
      reuseVertexGroupItem
        .setGroupRepresentativeVertexId(groupRepresentativeVertexId);
      reuseVertexGroupItem.setGroupLabel(groupLabel);
      reuseVertexGroupItem.setGroupPropertyValue(groupPropertyValue);
      reuseVertexGroupItem.setGroupCount(groupCount);

      collector.collect(reuseVertexGroupItem);
      reuseVertexGroupItem.reset();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SummarizationGroupCombine.class.getName();
  }
}
