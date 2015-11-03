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

package org.gradoop.model.impl.operators.logicalgraph.unary.summarization;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.util.FlinkConstants;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.functions.VertexToGroupVertexMapper;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.tuples.VertexForGrouping;
import org.gradoop.model.impl.operators.logicalgraph.unary.summarization.tuples
  .VertexWithRepresentative;

import java.util.List;

/**
 * Summarization implementation that does not require sorting of vertex groups.
 *
 * Algorithmic idea:
 *
 * 1) group vertices by label / property / both
 * 2) reduce groups
 * - create summarized vertex
 * - create list of vertex identifiers in the group
 * 3a) forward summarized vertices to vertex dataset
 * 3b) flat map build a {@link VertexWithRepresentative} per list item
 * 4) join output from 3b) with edges
 * - replace source / target vertex id with vertex group representative
 * 5) group edges on source/target vertex and possibly edge label / property
 * 6) build summarized edges
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class SummarizationGroupWithLists<
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
  public SummarizationGroupWithLists(String vertexGroupingKey,
    String edgeGroupingKey, boolean useVertexLabels, boolean useEdgeLabels) {
    super(vertexGroupingKey, edgeGroupingKey, useVertexLabels, useEdgeLabels);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Graph<Long, VD, ED> summarizeInternal(Graph<Long, VD, ED> graph) {
    /* build summarized vertices */
    // map vertex data to a smaller representation for grouping
    DataSet<VertexForGrouping> verticesForGrouping = graph.getVertices().map(
      new VertexToGroupVertexMapper<VD>(getVertexGroupingKey(),
        useVertexLabels()));

    // group vertices by either label or property or both
    UnsortedGrouping<VertexForGrouping> groupedVertices =
      groupVertices(verticesForGrouping);

    // create new summarized gelly vertices
    DataSet<Tuple2<Vertex<Long, VD>, List<Long>>>
      newVerticesWithGroupVertexIds =
      buildSummarizedVerticesWithVertexIdList(groupedVertices);

    DataSet<Vertex<Long, VD>> newVertices =
      newVerticesWithGroupVertexIds.map(new SummarizedVertexForwarder<VD>());

    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      newVerticesWithGroupVertexIds
        .flatMap(new VertexToGroupRepresentativeMapper<VD>());

    /* build summarized vertices */
    DataSet<Edge<Long, ED>> newEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return Graph.fromDataSet(newVertices, newEdges, graph.getContext());
  }


  /**
   * Constructs tuples each containing a new summarized vertices and a list
   * of vertex identifiers that summarized vertex represents.
   *
   * @param groupedSortedVertices grouped and sorted vertices
   * @return data set containing summarized vertex and its grouped vertex ids
   */
  protected DataSet<Tuple2<Vertex<Long, VD>, List<Long>>>
  buildSummarizedVerticesWithVertexIdList(
    UnsortedGrouping<VertexForGrouping> groupedSortedVertices) {
    return groupedSortedVertices.reduceGroup(
      new VertexGroupSummarizer<>(getVertexGroupingKey(), useVertexLabels(),
        config.getVertexFactory()));
  }

  /**
   * Creates a summarized vertex from a group of vertices and a list of
   * vertex identifiers that the summarized vertex represents.
   */
  private static class VertexGroupSummarizer<VD extends EPGMVertex> implements
    GroupReduceFunction<VertexForGrouping, Tuple2<Vertex<Long, VD>,
      List<Long>>>,
    ResultTypeQueryable<Tuple2<Vertex<Long, VD>, List<Long>>> {

    /**
     * Vertex data factory
     */
    private final EPGMVertexFactory<VD> vertexFactory;
    /**
     * Vertex property key to store group value
     */
    private final String groupPropertyKey;
    /**
     * True, if label shall be considered
     */
    private final boolean useLabel;
    /**
     * True, if property shall be considered.
     */
    private final boolean useProperty;
    /**
     * Avoid object instantiation in each reduce call.
     */
    private final Vertex<Long, VD> reuseVertex;
    /**
     * Avoid object instantiation.
     */
    private final Tuple2<Vertex<Long, VD>, List<Long>> reuseTuple;

    /**
     * Creates group reducer
     *
     * @param groupPropertyKey  vertex property key to store group value
     * @param useLabel          use vertex label
     * @param vertexFactory vertex data factory
     */
    public VertexGroupSummarizer(String groupPropertyKey, boolean useLabel,
      EPGMVertexFactory<VD> vertexFactory) {
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
      this.useProperty =
        groupPropertyKey != null && !"".equals(groupPropertyKey);
      this.vertexFactory = vertexFactory;
      this.reuseVertex = new Vertex<>();
      this.reuseTuple = new Tuple2<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<VertexForGrouping> vertices,
      Collector<Tuple2<Vertex<Long, VD>, List<Long>>> collector) throws
      Exception {
      Long newVertexID = 0L;
      String groupLabel = null;
      String groupValue = null;
      List<Long> groupedVertexIds = Lists.newArrayList();
      boolean initialized = false;
      for (VertexForGrouping v : vertices) {
        groupedVertexIds.add(v.getVertexId());
        if (!initialized) {
          // will be the minimum vertex id in the group
          newVertexID = v.getVertexId();
          // get label if necessary
          groupLabel =
            useLabel ? v.getGroupLabel() : GConstants.DEFAULT_VERTEX_LABEL;
          // get group value if necessary
          if (useProperty) {
            groupValue = getGroupProperty(v.getGroupPropertyValue());
          }
          initialized = true;
        }
      }
      VD vertexData =
        vertexFactory.createVertex(newVertexID, groupLabel);
      if (useProperty) {
        vertexData.setProperty(groupPropertyKey, groupValue);
      }
      vertexData
        .setProperty(COUNT_PROPERTY_KEY, (long) groupedVertexIds.size());
      vertexData.addGraph(FlinkConstants.SUMMARIZE_GRAPH_ID);

      reuseVertex.f0 = newVertexID;
      reuseVertex.f1 = vertexData;

      reuseTuple.f0 = reuseVertex;
      reuseTuple.f1 = groupedVertexIds;

      collector.collect(reuseTuple);
    }

    /**
     * Returns the group property value or the default value if vertices in
     * the group do not have the property.
     *
     * @param vertexPropertyValue vertex property value
     * @return final vertex group value
     */
    private String getGroupProperty(String vertexPropertyValue) {
      return (vertexPropertyValue != null) ? vertexPropertyValue : NULL_VALUE;
    }

    @Override
    public TypeInformation<Tuple2<Vertex<Long, VD>, List<Long>>>
    getProducedType() {
      return new TupleTypeInfo<>(
        new TupleTypeInfo<>(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
          TypeExtractor.getForClass(vertexFactory.getType())),
        TypeExtractor.getForClass(List.class));
    }
  }

  /**
   * This map function just forwards the summarized vertex contained in the
   * input tuple.
   *
   * @param <VD> vertex data type
   */
  @FunctionAnnotation.ForwardedFields("f0->*")
  private static class SummarizedVertexForwarder<VD extends EPGMVertex>
    implements
    MapFunction<Tuple2<Vertex<Long, VD>, List<Long>>, Vertex<Long, VD>> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex<Long, VD> map(
      Tuple2<Vertex<Long, VD>, List<Long>> vertexListTuple2) throws Exception {
      return vertexListTuple2.f0;
    }
  }

  /**
   * For a given grouped vertex and its a grouped vertex ids, for each
   * grouped vertex id, this function emits a tuple containing the vertex id
   * and the vertex id of the grouped vertex.
   *
   * @param <VD> vertex data type
   */
  private static class VertexToGroupRepresentativeMapper<VD extends EPGMVertex>
    implements FlatMapFunction<Tuple2<Vertex<Long, VD>, List<Long>>,
      VertexWithRepresentative> {

    /**
     * Avoid object instantiations.
     */
    private final VertexWithRepresentative reuseTuple;

    /**
     * Creates flat map function.
     */
    public VertexToGroupRepresentativeMapper() {
      reuseTuple = new VertexWithRepresentative();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flatMap(Tuple2<Vertex<Long, VD>, List<Long>> vertexListTuple2,
      Collector<VertexWithRepresentative> collector) throws Exception {
      for (Long vertexId : vertexListTuple2.f1) {
        reuseTuple.setVertexId(vertexId);
        reuseTuple.setGroupRepresentativeVertexId(vertexListTuple2.f0.getId());
        collector.collect(reuseTuple);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SummarizationGroupWithLists.class.getName();
  }
}
