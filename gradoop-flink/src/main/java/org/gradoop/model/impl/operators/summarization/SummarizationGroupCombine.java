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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.VertexDataFactory;
import org.gradoop.model.helper.FlinkConstants;

/**
 * Summarization implementation that does not require sorting of vertex groups.
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
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class SummarizationGroupCombine<VD extends VertexData, ED extends
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
  public SummarizationGroupCombine(String vertexGroupingKey,
    String edgeGroupingKey, boolean useVertexLabels, boolean useEdgeLabels) {
    super(vertexGroupingKey, edgeGroupingKey, useVertexLabels, useEdgeLabels);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Graph<Long, VD, ED> summarizeInternal(Graph<Long, VD, ED> graph) {
    /* build summarized vertices */
    // group vertices by either label or property or both
    UnsortedGrouping<Vertex<Long, VD>> groupedVertices = groupVertices(graph);

    // combine groups on partition basis
    GroupCombineOperator<Vertex<Long, VD>, VertexGroupItem> combineGroup =
      groupedVertices.combineGroup(
        new VertexGroupCombineFunction<VD>(getVertexGroupingKey(),
          useVertexLabels()));

    // group output again by either label or property or both
    UnsortedGrouping<VertexGroupItem> unsortedGrouping =
      (useVertexLabels() && useVertexProperty()) ?
        // group by label and property
        combineGroup.groupBy(2, 3) :
        // group by label
        (useVertexLabels() ? combineGroup.groupBy(2) :
          // group by property
          combineGroup.groupBy(3));

    // group reduce groups
    GroupReduceOperator<VertexGroupItem, VertexGroupItem> reduceGroup =
      unsortedGrouping.reduceGroup(new VertexGroupReduceFunction());

    // filter group representative tuples and build final vertices
    DataSet<Vertex<Long, VD>> summarizedVertices =
      reduceGroup.filter(new VertexGroupItemToSummarizedVertexFilter()).map(
        new VertexGroupItemToSummarizedVertexMapper<>(vertexDataFactory,
          getVertexGroupingKey(), useVertexLabels()));

    // filter vertex to representative tuples (used for edge join)
    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      reduceGroup.filter(new VertexGroupItemToRepresentativeFilter())
        .map(new VertexGroupItemToVertexWithRepresentativeMapper());

    // build summarized edges
    DataSet<Edge<Long, ED>> summarizedEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return Graph
      .fromDataSet(summarizedVertices, summarizedEdges, graph.getContext());
  }

  /**
   * Creates a new vertex representing a vertex group. The vertex stores the
   * group label, the group property value and the number of vertices in the
   * group.
   *
   * @param <VD> vertex data type
   */
  @FunctionAnnotation.ForwardedFields("f0")
  private static class VertexGroupItemToSummarizedVertexMapper<VD extends
    VertexData> implements
    MapFunction<VertexGroupItem, Vertex<Long, VD>>,
    ResultTypeQueryable<Vertex<Long, VD>> {

    /**
     * Vertex data factory.
     */
    private final VertexDataFactory<VD> vertexDataFactory;
    /**
     * Vertex property key used for grouping.
     */
    private final String groupPropertyKey;
    /**
     * True, if the vertex label shall be considered.
     */
    private final boolean useLabel;
    /**
     * True, if the vertex property shall be considered.
     */
    private final boolean useProperty;
    /**
     * Avoid object instantiations.
     */
    private final Vertex<Long, VD> reuseVertex;

    /**
     * Creates map function.
     *
     * @param vertexDataFactory vertex data factory
     * @param groupPropertyKey  vertex property key for grouping
     * @param useLabel          true, if vertex label shall be considered
     */
    private VertexGroupItemToSummarizedVertexMapper(
      VertexDataFactory<VD> vertexDataFactory, String groupPropertyKey,
      boolean useLabel) {
      this.vertexDataFactory = vertexDataFactory;
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
      useProperty = groupPropertyKey != null && !"".equals(groupPropertyKey);
      reuseVertex = new Vertex<>();
    }

    /**
     * Creates a {@link VertexData} object from the given {@link
     * VertexGroupItem} and returns a new {@link Vertex}.
     *
     * @param vertexGroupItem vertex group item
     * @return vertex including new vertex data
     * @throws Exception
     */
    @Override
    public Vertex<Long, VD> map(VertexGroupItem vertexGroupItem) throws
      Exception {
      VD summarizedVertexData =
        vertexDataFactory.createVertexData(vertexGroupItem.getVertexId());
      if (useLabel) {
        summarizedVertexData.setLabel(vertexGroupItem.getGroupLabel());
      }
      if (useProperty) {
        summarizedVertexData.setProperty(groupPropertyKey,
          vertexGroupItem.getGroupPropertyValue());
      }
      summarizedVertexData
        .setProperty(COUNT_PROPERTY_KEY, vertexGroupItem.getGroupCount());
      summarizedVertexData.addGraph(FlinkConstants.SUMMARIZE_GRAPH_ID);

      reuseVertex.setId(vertexGroupItem.getVertexId());
      reuseVertex.setValue(summarizedVertexData);
      return reuseVertex;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public TypeInformation<Vertex<Long, VD>> getProducedType() {
      return new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(vertexDataFactory.getType()));
    }
  }

  /**
   * Filter those tuples which are used to create new summarized vertices.
   * Those tuples have a group count > 0.
   */
  @FunctionAnnotation.ForwardedFields("*->*")
  private static class VertexGroupItemToSummarizedVertexFilter implements
    FilterFunction<VertexGroupItem> {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean filter(VertexGroupItem vertexGroupItem) throws Exception {
      return !vertexGroupItem.getGroupCount().equals(0L);
    }
  }

  /**
   * Filter those tuples which are used to create {@link
   * VertexWithRepresentative} objects.
   */
  @FunctionAnnotation.ForwardedFields("*->*")
  private static class VertexGroupItemToRepresentativeFilter implements
    FilterFunction<VertexGroupItem> {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean filter(VertexGroupItem vertexGroupItem) throws Exception {
      return vertexGroupItem.getGroupCount().equals(0L);
    }
  }

  /**
   * Maps a {@link VertexGroupItem} to a {@link VertexWithRepresentative}.
   */
  @FunctionAnnotation.ForwardedFields("f0;f1")
  private static class VertexGroupItemToVertexWithRepresentativeMapper
    implements
    MapFunction<VertexGroupItem, VertexWithRepresentative> {

    /**
     * Avoid object instantiation.
     */
    private final VertexWithRepresentative reuseTuple;

    /**
     * Creates mapper.
     */
    private VertexGroupItemToVertexWithRepresentativeMapper() {
      this.reuseTuple = new VertexWithRepresentative();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VertexWithRepresentative map(VertexGroupItem vertexGroupItem) throws
      Exception {
      reuseTuple.setVertexId(vertexGroupItem.getVertexId());
      reuseTuple.setGroupRepresentativeVertexId(
        vertexGroupItem.getGroupRepresentativeVertexId());
      return reuseTuple;
    }
  }

  /**
   * Takes the output tuples of the group combine step and chooses a group
   * representative (group representative of first incoming tuple).
   * Replaces the group representative of all following tuples with group
   * final group representative.
   *
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
      Long groupRepresentativeVertexId = null;
      Long groupCount = 0L;
      String groupLabel = null;
      String groupPropertyValue = null;
      boolean firstElement = true;

      for (VertexGroupItem vertexGroupItem : vertexGroupItems) {
        if (firstElement) {
          // take final group representative vertex id from first tuple
          groupRepresentativeVertexId =
            vertexGroupItem.getGroupRepresentativeVertexId();
          firstElement = false;

          if (vertexGroupItem.getGroupLabel() != null) {
            groupLabel = vertexGroupItem.getGroupLabel();
          }

          if (vertexGroupItem.getGroupPropertyValue() != null) {
            groupPropertyValue = vertexGroupItem.getGroupPropertyValue();
          }
        }

        reuseVertexGroupItem.setVertexId(vertexGroupItem.getVertexId());
        reuseVertexGroupItem
          .setGroupRepresentativeVertexId(groupRepresentativeVertexId);

        if (vertexGroupItem.getGroupCount().equals(0L)) {
          collector.collect(reuseVertexGroupItem);
        } else {
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

  private static class VertexGroupCombineFunction<VD extends VertexData>
    implements
    GroupCombineFunction<Vertex<Long, VD>, VertexGroupItem> {

    private final String groupPropertyKey;
    private final boolean useLabel;
    private final boolean useProperty;
    private final VertexGroupItem reuseVertexGroupItem;

    public VertexGroupCombineFunction(String groupPropertyKey,
      boolean useLabel) {
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
      this.useProperty =
        groupPropertyKey != null && !"".equals(groupPropertyKey);
      this.reuseVertexGroupItem = new VertexGroupItem();
    }

    @Override
    public void combine(Iterable<Vertex<Long, VD>> vertices,
      Collector<VertexGroupItem> collector) throws Exception {

      Long groupRepresentativeVertexId = null;
      Long groupPartitionCount = 0L;
      String groupLabel;
      String groupPropertyValue;
      boolean firstElement = true;

      for (Vertex<Long, VD> vertex : vertices) {
        reuseVertexGroupItem.setVertexId(vertex.getId());
        if (firstElement) {
          groupRepresentativeVertexId = vertex.getId();
          groupLabel = getGroupLabel(vertex.getValue());
          groupPropertyValue = getGroupPropertyValue(vertex.getValue());


          reuseVertexGroupItem
            .setGroupRepresentativeVertexId(groupRepresentativeVertexId);
          reuseVertexGroupItem.setGroupLabel(groupLabel);
          reuseVertexGroupItem.setGroupPropertyValue(groupPropertyValue);

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

    private String getGroupLabel(VD vertexData) {
      return useLabel ? vertexData.getLabel() : null;
    }

    /**
     * Returns the group property value or the default value if vertices in
     * the group do not have the property.
     *
     * @param vertexData vertex data object of the summarized vertex
     * @return vertex group value
     */
    private String getGroupPropertyValue(VD vertexData) {
      if (useProperty && vertexData.getProperty(groupPropertyKey) != null) {
        return vertexData.getProperty(groupPropertyKey).toString();
      } else if (useProperty) {
        // just in case we need to group on property and the vertex has none
        return NULL_VALUE;
      } else {
        return null;
      }
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
