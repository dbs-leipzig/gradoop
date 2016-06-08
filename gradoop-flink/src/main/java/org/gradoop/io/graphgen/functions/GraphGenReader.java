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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.graphgen.functions;

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.io.graphgen.GraphGenStringToCollection;
import org.gradoop.io.graphgen.tuples.GraphGenEdge;
import org.gradoop.io.graphgen.tuples.GraphGenGraph;
import org.gradoop.io.graphgen.tuples.GraphGenVertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class contains static classes to generate datasets of EPGMGraphHead,
 * EPGMVertex and EPGMEdge. Therefore a mapping from a graphgen collection to
 * a dataset of EPGMElements is provided. As well as a filter on this element
 * and a mapper from the general EPGMElement to the specified EPGMElement.
 * Additionally it is possible to generate graph transactions from a GraphGen
 * file.
 */
public class GraphGenReader {

  /**
   * Reads graph imported from a GraphGen file. The result of the mapping is a
   * dataset of of graph transactions, with each GraphTransaction consisting
   * of a graph head, a set of vertices and a set of edges.
   *
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   */
  public static class GraphGenCollectionToGraphTransactions<G extends
    EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> implements
    FlatMapFunction<Tuple2<LongWritable, Text>, GraphTransaction<G, V, E>> {

    /**
     * Creates graph data objects
     */
    private final EPGMGraphHeadFactory<G> graphHeadFactory;

    /**
     * Creates graph data objects
     */
    private final EPGMVertexFactory<V> vertexFactory;

    /**
     * Creates graph data objects
     */
    private final EPGMEdgeFactory<E> edgeFactory;

    /**
     * Reduce object instantiation.
     */
    private GraphTransaction<G, V, E> graphTransaction;

    /**
     * Creates a flatmap function.
     *
     * @param graphHeadFactory graph head data factory
     * @param vertexFactory    vertex data factory
     * @param edgeFactory      edge data factory
     */
    public GraphGenCollectionToGraphTransactions(
      EPGMGraphHeadFactory<G> graphHeadFactory, EPGMVertexFactory<V>
      vertexFactory, EPGMEdgeFactory<E> edgeFactory) {
      this.graphHeadFactory = graphHeadFactory;
      this.vertexFactory = vertexFactory;
      this.edgeFactory = edgeFactory;

      Set<V> vertices = new HashSet<>();
      Set<E> edges = new HashSet<>();
      V source = this.vertexFactory.createVertex();
      V target = this.vertexFactory.createVertex();
      vertices.add(source);
      vertices.add(target);
      edges.add(this.edgeFactory.createEdge(source.getId(), target.getId()));

      graphTransaction = new GraphTransaction<G, V, E>(this
        .graphHeadFactory.initGraphHead(GradoopId.get()), vertices, edges);
    }

    /**
     * Constructs a dataset containing GraphTransaction(s).
     *
     * @param inputTuple consists of a key(LongWritable) and a value(Text)
     * @param collector of GraphTransaction(s)
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<LongWritable, Text> inputTuple,
      Collector<GraphTransaction<G, V, E>> collector) throws Exception {
      String graphString = inputTuple.getField(1).toString();
      GraphGenStringToCollection graphGenStringToCollection = new
        GraphGenStringToCollection();
      graphGenStringToCollection.setContent(graphString);

      Collection<GraphGenGraph> graphCollection;
      graphCollection = graphGenStringToCollection.getGraphCollection();

      Set<V> vertices = new HashSet<>();
      Set<E> edges = new HashSet<>();

      GradoopId id;
      String label;
      GradoopId targetId;
      GradoopIdSet graphs = new GradoopIdSet();
      Map<Integer, GradoopId> integerGradoopIdMapVertices;

      for (GraphGenGraph graph : graphCollection) {
        integerGradoopIdMapVertices = new HashedMap();
        graphs.clear();
        vertices.clear();
        edges.clear();

        id = GradoopId.get();
        graphs.add(id);

        for (GraphGenVertex vertex : graph.getGraphVertices()) {
          id = GradoopId.get();
          integerGradoopIdMapVertices.put(vertex.getId(), id);
          label = vertex.getLabel();
          vertices.add(this.vertexFactory.initVertex(id, label, graphs));
        }

        for (GraphGenEdge edge :graph.getGraphEdges()) {
          id = integerGradoopIdMapVertices.get(edge.getSourceId());
          targetId = integerGradoopIdMapVertices.get(edge.getTargetId());
          label = edge.getLabel();
          edges.add(this.edgeFactory.createEdge(label, id, targetId, graphs));
        }
        collector.collect(new GraphTransaction<G, V, E>(this
          .graphHeadFactory.initGraphHead(id), vertices, edges));
      }
    }

    /**
     * Returns the produced type information (GraphTransaction<G, V, E>) of the
     * flatmap.
     *
     * @return type information of GraphTransaction<G, V, E>
     */
    public TypeInformation<GraphTransaction<G, V, E>> getProducedType() {
      return TypeExtractor.getForObject(this.graphTransaction);
    }
  }

  /**
   * Reads graph imported from a GraphGen file. The result of the mapping is a
   * dataset containing EPGMElements(EPGMGraphHead, EPGMVertex and EPGMEdge)
   * as EPGMElements.
   *
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   */
  public static class GraphGenCollectionToEPGMElementsMapper<G extends
    EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> implements
    FlatMapFunction<Tuple2<LongWritable, Text>, EPGMElement> {

    /**
     * Creates graph data objects
     */
    private final EPGMGraphHeadFactory<G> graphHeadFactory;

    /**
     * Creates graph data objects
     */
    private final EPGMVertexFactory<V> vertexFactory;

    /**
     * Creates graph data objects
     */
    private final EPGMEdgeFactory<E> edgeFactory;

    /**
     * Creates a flatmap function.
     *
     * @param graphHeadFactory graph head data factory
     * @param vertexFactory    vertex data factory
     * @param edgeFactory      edge data factory
     */
    public GraphGenCollectionToEPGMElementsMapper(
      EPGMGraphHeadFactory<G> graphHeadFactory, EPGMVertexFactory<V>
      vertexFactory, EPGMEdgeFactory<E> edgeFactory) {
      this.graphHeadFactory = graphHeadFactory;
      this.vertexFactory = vertexFactory;
      this.edgeFactory = edgeFactory;
    }

    /**
     * Constructs a dataset containing EPGMElements(EPGMGraphHead,
     * EPGMVertex and EPGMEdge) as EPGMElement.
     *
     * @param inputTuple consists of a key(LongWritable) and a value(Text)
     * @param collector  of EPGMElements
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<LongWritable, Text> inputTuple,
      Collector<EPGMElement> collector) throws Exception {
      String graphString = inputTuple.getField(1).toString();
      GraphGenStringToCollection graphGenStringToCollection = new
        GraphGenStringToCollection();
      graphGenStringToCollection.setContent(graphString);

      Collection<GraphGenGraph> graphCollection;
      graphCollection = graphGenStringToCollection.getGraphCollection();

      GradoopId id;
      String label;
      GradoopId targetId;
      GradoopIdSet graphs = new GradoopIdSet();
      Map<Integer, GradoopId> integerGradoopIdMapVertices;

      for (GraphGenGraph graph : graphCollection) {
        integerGradoopIdMapVertices = new HashedMap();
        graphs.clear();

        id = GradoopId.get();
        graphs.add(id);
        collector.collect(this.graphHeadFactory.initGraphHead(id));

        for (GraphGenVertex vertex : graph.getGraphVertices()) {
          id = GradoopId.get();
          integerGradoopIdMapVertices.put(vertex.getId(), id);
          label = vertex.getLabel();
          collector.collect(this.vertexFactory.initVertex(id, label, graphs));
        }

        for (GraphGenEdge edge :graph.getGraphEdges()) {
          id = integerGradoopIdMapVertices.get(edge.getSourceId());
          targetId = integerGradoopIdMapVertices.get(edge.getTargetId());
          label = edge.getLabel();
          collector.collect(this.edgeFactory.createEdge(label, id, targetId,
            graphs));
        }
      }
    }
  }

  /**
   * Accepts only those EPGMElements where the element is instance of
   * committed class.
   */
  public static class GraphGenFilterOnClass implements
    FilterFunction<EPGMElement> {

    /**
     * The class on which the filter works.
     */
    private Class filterClass;

    /**
     * Creates a new filter and saves the class to be filtered on.
     *
     * @param filterClass the class to be filtered on
     */
    public GraphGenFilterOnClass(Class filterClass) {
      this.filterClass = filterClass;
    }

    /**
     * The filter function accepts only those elements, where the element is
     * instance of the class committed on construction.
     *
     * @param element the tuple to be filtered
     * @return true if class field is equal to class of compromise
     * @throws Exception
     */
    @Override
    public boolean filter(EPGMElement element) throws
      Exception {
      return filterClass.isInstance(element);
    }
  }

  /**
   * Returns DataSet<El> where El is an EPGMGraphHead, an EPGMVertex or an
   * EPGMEdge. The element itself is cast from EPGMElement to El.
   *
   * @param <El> the type to be returned
   */
  public static class GraphGenEPGMElementMapper<El extends EPGMElement>
    implements
    MapFunction<EPGMElement, El> {

    /**
     * Returns an EPGMElement of type El which has been cast from the
     * EPGMElement.
     *
     * @param element EPGMElement
     * @return EPGMElement of type El cast from EPGMElement
     * @throws Exception
     */
    @Override
    public El map(EPGMElement element) throws
      Exception {
      return (El) element;
    }
  }

}
