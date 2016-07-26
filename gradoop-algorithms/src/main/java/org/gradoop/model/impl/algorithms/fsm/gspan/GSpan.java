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

package org.gradoop.model.impl.algorithms.fsm.gspan;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.comparators.DFSCodeSiblingComparator;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.DirectedDFSStep;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanEdge;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.UndirectedDFSStep;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulation of all logic of the gSpan algorithm for mining frequent
 * subgraphs in a collection of graphs. File is separated into methods for
 * graph construction, pattern growth, minimal DFS code validation and pattern
 * matching
 */
public class GSpan {

  // GRAPH CONSTRUCTION

  /**
   * Creates the gSpan mining representation of a graph transaction from a
   * given collection of Gradoop edge triples.
   *
   * @param <T> edge triple type
   * @param <IDT> id type
   * @param triples the graphs edges
   * @param fsmConfig FSM configuration
   * @return graph transaction
   */
  public static <T extends EdgeTriple<IDT>, IDT> GSpanGraph
  createGSpanGraph(Iterable<T> triples, FSMConfig fsmConfig) {

    // replace GradoopIds by Integer Ids
    List<GSpanEdge> edges = Lists.newArrayList();
    List<AdjacencyList> adjacencyLists = Lists.newArrayList();
    createAdjacencyListsAndEdges(triples, adjacencyLists, edges);

    return createGSpanGraph(adjacencyLists, edges, fsmConfig);
  }

  /**
   * Creates the gSpan mining representation of a graph transaction from an
   * existing encoded subgraph.
   *
   * @param subgraph encodes subgraph
   * @param fsmConfig FSM configuration
   * @return graph transaction
   */
  private static GSpanGraph createGSpanGraph(DFSCode subgraph,
    FSMConfig fsmConfig) {

    // turn DFS edges into gSpan edges
    List<DFSStep> steps = subgraph.getSteps();
    List<AdjacencyList> adjacencyLists = Lists.newArrayList();
    List<GSpanEdge> edges = Lists.newArrayListWithExpectedSize(steps.size());
    createAdjacencyListsAndEdges(steps, adjacencyLists, edges);

    return createGSpanGraph(adjacencyLists, edges, fsmConfig);
  }

  /**
   * Creates the gSpan mining representation of a graph transaction from a
   * give list of adjacency lists and edges.
   *
   * @param adjacencyLists adjacency lists
   * @param edges edges
   * @param fsmConfig FSM configuration
   * @return graph transaction
   */
  private static GSpanGraph createGSpanGraph(List<AdjacencyList> adjacencyLists,
    List<GSpanEdge> edges, FSMConfig fsmConfig) {
    // sort by min vertex label
    Collections.sort(edges);

    // create adjacency lists and 1-edge subgraphs with their embeddings

    Map<DFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      Maps.newHashMap();

    Iterator<GSpanEdge> iterator = edges.iterator();
    GSpanEdge lastEdge = iterator.next();

    Collection<DFSEmbedding> embeddings =
      createSingleEdgeSubgraphEmbeddings(codeEmbeddings, lastEdge, fsmConfig);

    while (iterator.hasNext()) {
      GSpanEdge edge = iterator.next();

      // add embedding of 1-edge code
      if (edge.compareTo(lastEdge) == 0) {
        embeddings.add(createSingleEdgeEmbedding(edge));
      } else {
        embeddings = createSingleEdgeSubgraphEmbeddings(codeEmbeddings, edge,
          fsmConfig);
        lastEdge = edge;
      }
    }

    return new GSpanGraph(adjacencyLists, codeEmbeddings);
  }

  /**
   * turns edge triples into gSpan edges,
   * i.e., replaces GradoopIds by local integer ids
   *
   * @param iterable edge triples with GradoopIds
   * @param adjacencyLists adjacency lists
   * @param edges  @return gSpan edges
   * @param <T> edge triple type
   * @param <IDT> id type
   */
  private static <T extends EdgeTriple<IDT>, IDT> void
  createAdjacencyListsAndEdges(final Iterable<T> iterable,
    final List<AdjacencyList> adjacencyLists, final List<GSpanEdge> edges) {

    Map<IDT, Integer> vertexIdMap = Maps.newHashMap();
    int vertexId = 0;

    int edgeId = 0;
    for (EdgeTriple<IDT> triple : iterable) {

      Integer edgeLabel = triple.getEdgeLabel();

      IDT sourceGradoopId = triple.getSourceId();
      Integer sourceId = vertexIdMap.get(sourceGradoopId);
      Integer sourceLabel = triple.getSourceLabel();

      if (sourceId == null) {
        sourceId = vertexId;
        vertexIdMap.put(sourceGradoopId, sourceId);
        vertexId++;
      }

      IDT targetGradoopId = triple.getTargetId();
      Integer targetId = vertexIdMap.get(targetGradoopId);
      Integer targetLabel = triple.getTargetLabel();

      if (targetId == null) {
        targetId = vertexId;
        vertexIdMap.put(targetGradoopId, targetId);

        vertexId++;
      }

      addNewEdgeAndAdjacencyListEntries(edges, adjacencyLists,
        sourceId, sourceLabel, edgeId, edgeLabel, targetId, targetLabel);

      edgeId++;
    }
  }

  /**
   * Creates a new gSpan edge and two adjacency list entries for given labels
   * and identifiers. Edge will be added to an edge lists and adjacency list
   * entries to an adjacency list.
   *
   * @param edges edges
   * @param adjacencyLists adjacency lists
   * @param sourceId source vertex id
   * @param sourceLabel source vertex label
   * @param edgeId edge id
   * @param edgeLabel edge label
   * @param targetId target id
   * @param targetLabel target label
   */
  private static void addNewEdgeAndAdjacencyListEntries(
    final List<GSpanEdge> edges,
    final List<AdjacencyList> adjacencyLists,
    int sourceId, int sourceLabel,
    int edgeId, int edgeLabel,
    int targetId, int targetLabel) {

    edges.add(new GSpanEdge(
      sourceId, sourceLabel, edgeId, edgeLabel, targetId, targetLabel));

    if (sourceId <= targetId) {
      addAdjacencyListEntry(adjacencyLists,
        sourceId, sourceLabel, true, edgeId, edgeLabel, targetId, targetLabel);

      addAdjacencyListEntry(adjacencyLists,
        targetId, targetLabel, false, edgeId, edgeLabel, sourceId, sourceLabel);
    } else {
      addAdjacencyListEntry(adjacencyLists,
        targetId, targetLabel, false, edgeId, edgeLabel, sourceId, sourceLabel);

      addAdjacencyListEntry(adjacencyLists,
        sourceId, sourceLabel, true, edgeId, edgeLabel, targetId, targetLabel);
    }
  }

  /**
   * Creates a new adjacency list entry and adds it to its adjacency list.
   *
   * @param adjacencyLists adjacency lists
   * @param fromId entry owning vertex id
   * @param fromLabel entry owning vertex label
   * @param outgoing true, if outgoing entry, false, if incoming
   * @param edgeId edge id
   * @param edgeLabel edge label
   * @param toId referenced vertex id
   * @param toLabel reference vertex label
   */
  private static void addAdjacencyListEntry(
    final List<AdjacencyList> adjacencyLists,
    int fromId, int fromLabel,
    boolean outgoing, int edgeId, int edgeLabel,
    int toId, int toLabel) {

    AdjacencyList adjacencyList;

    if (fromId >= adjacencyLists.size()) {
      adjacencyList = new AdjacencyList(fromLabel);
      adjacencyLists.add(adjacencyList);
    } else {
      adjacencyList = adjacencyLists.get(fromId);
    }

    adjacencyList.getEntries().add(new AdjacencyListEntry(
      outgoing, edgeId, edgeLabel, toId, toLabel));
  }

  /**
   * Creates adjacency lists and gSpan edges for a list of DFS steps.
   *  @param steps DFS steps
   * @param adjacencyLists adjacency lists
   * @param edges edges
   */
  private static void createAdjacencyListsAndEdges(final List<DFSStep> steps,
    final List<AdjacencyList> adjacencyLists, final List<GSpanEdge> edges) {

    int edgeId = 0;
    for (DFSStep step : steps) {

      Integer edgeLabel = step.getEdgeLabel();

      Integer sourceId;
      Integer sourceLabel;

      Integer targetId;
      Integer targetLabel;

      if (step.isOutgoing()) {
        sourceId = step.getFromTime();
        sourceLabel = step.getFromLabel();
        targetId = step.getToTime();
        targetLabel = step.getToLabel();
      } else {
        sourceId = step.getToTime();
        sourceLabel = step.getToLabel();
        targetId = step.getFromTime();
        targetLabel = step.getFromLabel();
      }

      addNewEdgeAndAdjacencyListEntries(edges, adjacencyLists,
        sourceId, sourceLabel, edgeId, edgeLabel, targetId, targetLabel);

      edgeId++;
    }
  }

  /**
   * Creates a new entry for single edge subgraph including an initial
   * embedding for a gibe edge.
   *
   * @param subgraphEmbeddings subgraph-embeddings map
   * @param edge edge
   * @param fsmConfig FSM configuration
   * @return collection of embeddings
   */
  private static Collection<DFSEmbedding> createSingleEdgeSubgraphEmbeddings(
    Map<DFSCode, Collection<DFSEmbedding>> subgraphEmbeddings, GSpanEdge edge,
    FSMConfig fsmConfig) {

    DFSCode subgraph = createSingleEdgeSubgraph(edge, fsmConfig);
    DFSEmbedding embedding = createSingleEdgeEmbedding(edge);
    Collection<DFSEmbedding> embeddings = Lists.newArrayList(embedding);
    subgraphEmbeddings.put(subgraph, embeddings);

    return embeddings;
  }

  /**
   * Creates a single edge embedding for a given edge.
   *
   * @param edge edge
   * @return embedding
   */
  private static DFSEmbedding createSingleEdgeEmbedding(final GSpanEdge edge) {
    List<Integer> vertexTimes;

    if (edge.isLoop()) {
      vertexTimes = Lists.newArrayList(edge.getSourceId());
    } else if (edge.getSourceLabel() <= edge.getTargetLabel()) {
      vertexTimes = Lists.newArrayList(edge.getSourceId(), edge.getTargetId());
    } else {
      vertexTimes = Lists.newArrayList(edge.getTargetId(), edge.getSourceId());
    }

    return new DFSEmbedding(vertexTimes, Lists.newArrayList(edge.getEdgeId()));
  }

  /**
   * Create a single edge Subgraph for a given edge.
   *
   * @param edge edge
   * @param fsmConfig FSM configuration
   * @return subgraph
   */
  private static DFSCode createSingleEdgeSubgraph(final GSpanEdge edge,
    FSMConfig fsmConfig) {

    int sourceLabel = edge.getSourceLabel();
    int edgeLabel = edge.getLabel();
    int targetLabel = edge.getTargetLabel();

    DFSStep step;

    if (fsmConfig.isDirected()) {
      if (edge.isLoop()) {
        step = new DirectedDFSStep(
          0, sourceLabel, true, edgeLabel, 0, sourceLabel);
      } else if (edge.sourceIsMinimumLabel()) {
        step = new DirectedDFSStep(
          0, sourceLabel, true, edgeLabel, 1, targetLabel);
      } else {
        step = new DirectedDFSStep(
          0, targetLabel, false, edgeLabel, 1, sourceLabel);
      }
    } else {
      if (edge.isLoop()) {
        step = new UndirectedDFSStep(
          0, sourceLabel, edgeLabel, 0, sourceLabel);
      } else if (edge.sourceIsMinimumLabel()) {
        step = new UndirectedDFSStep(
          0, sourceLabel, edgeLabel, 1, targetLabel);
      } else {
        step = new UndirectedDFSStep(
          0, targetLabel, edgeLabel, 1, sourceLabel);
      }
    }

    return new DFSCode(step);
  }

  // PATTERN GROWTH

  /**
   * Core of gSpan pattern growth.
   * Grows all children of supported frequent subgraphs.
   *
   * @param graph graph
   * @param frequentParentSubgraphs frequent subgraphs
   * @param fsmConfig FSM configuration
   */
  public static void growEmbeddings(final GSpanGraph graph,
    Collection<DFSCode> frequentParentSubgraphs, FSMConfig fsmConfig) {
    Map<DFSCode, Collection<DFSEmbedding>> childCodeEmbeddings = null;

    for (DFSCode parentSubgraph : frequentParentSubgraphs) {
      Collection<DFSEmbedding> parentEmbeddings =
        graph.getSubgraphEmbeddings().get(parentSubgraph);

      if (parentEmbeddings != null) {
        int minVertexLabel = parentSubgraph.getMinVertexLabel();

        List<Integer> rightmostPath =
          parentSubgraph.getRightMostPathVertexTimes();

        // for each embedding
        for (DFSEmbedding parentEmbedding : parentEmbeddings) {

          // first iterated vertex is rightmost
          Boolean rightMostVertex = true;

          Set<Integer> visitedEdges =
            Sets.newHashSet(parentEmbedding.getEdgeTimes());

          Map<Integer, Integer> vertexIdTime =
            createVertexTimeIndex(parentEmbedding);

          // for each time on rightmost path
          for (Integer fromVertexTime : rightmostPath) {
            Integer fromVertexId =
              parentEmbedding.getVertexTimes().get(fromVertexTime);

            // query fromVertex data
            AdjacencyList adjacencyList =
              graph.getAdjacencyLists().get(fromVertexId);

            Integer fromVertexLabel = adjacencyList.getFromVertexLabel();

            // for each incident edge
            for (AdjacencyListEntry entry : adjacencyList.getEntries()) {

              // if valid extension for branch
              int toVertexLabel = entry.getToVertexLabel();

              if (toVertexLabel >= minVertexLabel) {

                Integer edgeId = entry.getEdgeId();

                // if edge not already contained
                if (!visitedEdges.contains(edgeId)) {

                  // query toVertexData
                  Integer toVertexId = entry.getToVertexId();
                  Integer toVertexTime = vertexIdTime.get(toVertexId);

                  boolean forward = toVertexTime == null;

                  // PRUNING : grow only forward
                  // or backward from rightmost vertex
                  if (forward || rightMostVertex) {

                    DFSEmbedding childEmbedding =
                      DFSEmbedding.deepCopy(parentEmbedding);
                    DFSCode childCode = DFSCode.deepCopy(parentSubgraph);

                    // add new vertex to embedding for forward steps
                    if (forward) {
                      childEmbedding.getVertexTimes().add(toVertexId);
                      toVertexTime = vertexIdTime.size();
                    }

                    DFSStep dfsStep;

                    if (fsmConfig.isDirected()) {
                      dfsStep = new DirectedDFSStep(
                        fromVertexTime, fromVertexLabel,
                        entry.isOutgoing(), entry.getEdgeLabel(),
                        toVertexTime, toVertexLabel);
                    } else {
                      dfsStep = new UndirectedDFSStep(
                        fromVertexTime, fromVertexLabel,
                        entry.getEdgeLabel(),
                        toVertexTime, toVertexLabel);
                    }

                    childCode.getSteps().add(dfsStep);

                    childEmbedding.getEdgeTimes().add(edgeId);

                    childCodeEmbeddings = addCodeEmbedding(
                      childCodeEmbeddings, childCode, childEmbedding);
                  }
                }
              }
            }
            rightMostVertex = false;
          }
        }
      }
    }
    graph.setSubgraphEmbeddings(childCodeEmbeddings);
  }

  /**
   * Creates an index of the vertex time of each mapped vertex id.
   *
   * @param embedding embedding containing the time-vertex mapping
   * @return vertex-time mapping
   */
  private static Map<Integer, Integer> createVertexTimeIndex(
    DFSEmbedding embedding) {

    Map<Integer, Integer> vertexTimes =
      Maps.newHashMapWithExpectedSize(embedding.getVertexTimes().size());

    int vertexTime = 0;
    for (int vertexId : embedding.getVertexTimes()) {
      vertexTimes.put(vertexId, vertexTime);
      vertexTime++;
    }
    return vertexTimes;
  }

  /**
   * Returns a new comparator for subgraph siblings depending on the given
   * configuration.
   * @param fsmConfig FSM configuration
   * @return comparator
   */
  private static DFSCodeSiblingComparator getSiblingComparator(
    FSMConfig fsmConfig) {

    return new DFSCodeSiblingComparator(fsmConfig.isDirected());
  }

  /**
   * Adds a subgraph and one of its embeddings to a map of subgraphs and
   * their embeddings. Creates a new map, if null. Creates a new entry, if DFS
   * code not already contained.
   *
   * @param subgraphEmbeddings subgraph - embeddings - map
   * @param subgraph subgraph
   * @param embedding embedding
   * @return updated input map
   */
  private static Map<DFSCode, Collection<DFSEmbedding>> addCodeEmbedding(
    Map<DFSCode, Collection<DFSEmbedding>> subgraphEmbeddings,
    DFSCode subgraph, DFSEmbedding embedding) {

    Collection<DFSEmbedding> embeddings;

    if (subgraphEmbeddings == null) {
      subgraphEmbeddings = Maps.newHashMap();
      subgraphEmbeddings.put(subgraph, Lists.newArrayList(embedding));
    } else {
      embeddings = subgraphEmbeddings.get(subgraph);

      if (embeddings == null) {
        subgraphEmbeddings.put(subgraph, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }
    return subgraphEmbeddings;
  }

  // MINIMAL DFS CODE VALIDATION

  /**
   * Checks, if a subgraph is minimal.
   *
   * @param subgraph subgraph
   * @param fsmConfig FSM configuration
   * @return true, if minimal, false otherwise
   */
  public static boolean isMinimal(
    DFSCode subgraph, FSMConfig fsmConfig) {

    GSpanGraph graph = createGSpanGraph(subgraph, fsmConfig);
    DFSCode minDfsCode = calculateMinDFSCode(graph, fsmConfig);

    return subgraph.equals(minDfsCode);
  }

  /**
   * Determines the minimal subgraph of a given graph.
   *
   * @param graph graph
   * @param fsmConfig FSM configuration
   * @return minimal subgraph
   */
  public static DFSCode calculateMinDFSCode(
    GSpanGraph graph, FSMConfig fsmConfig) {

    DFSCode minDfsCode = null;

    while (graph.hasGrownSubgraphs()) {
      Set<DFSCode> grownSubgraphs =
        graph.getSubgraphEmbeddings().keySet();

      minDfsCode = selectMinDFSCode(grownSubgraphs, fsmConfig);

      growEmbeddings(graph, Lists.newArrayList(minDfsCode), fsmConfig);
    }
    return minDfsCode;
  }

  /**
   * Selectis the minimal subgraph from a given collection.
   *
   * @param subgraphs collection of subgraphs
   * @param fsmConfig FSM configuration
   * @return minimal subgraph
   */
  public static DFSCode selectMinDFSCode(
    final Collection<DFSCode> subgraphs, final FSMConfig fsmConfig) {

    Iterator<DFSCode> iterator = subgraphs.iterator();

    DFSCode minDfsCode = iterator.next();

    for (DFSCode nextDfsCode : subgraphs) {
      if (getSiblingComparator(fsmConfig).compare(nextDfsCode, minDfsCode) < 0)
      {
        minDfsCode = nextDfsCode;
      }
    }

    return minDfsCode;
  }

  // PATTERN MATCHING

  /**
   * Checks, if a graph contains a subgraph (pattern matching).
   * @param graph search graph
   * @param subgraph subgraph
   * @param fsmConfig FSM configuration
   * @return true, if subgraph is contained, false, otherwise
   */
  public static boolean contains(
    GSpanGraph graph, DFSCode subgraph, FSMConfig fsmConfig) {


    Iterator<DFSStep> iterator = subgraph.getSteps().iterator();

    DFSStep step = iterator.next();

    Collection<DFSEmbedding> parentEmbeddings = Lists.newArrayList();
    int fromVertexId = 0;
    for (AdjacencyList adjacencyList : graph.getAdjacencyLists()) {
      if (step.getFromLabel().equals(adjacencyList.getFromVertexLabel())) {
        for (AdjacencyListEntry entry : adjacencyList.getEntries()) {
          if (step.isOutgoing() == entry.isOutgoing() &&
            step.getEdgeLabel().equals(entry.getEdgeLabel()) &&
            step.getToLabel().equals(entry.getToVertexLabel())) {

            List<Integer> vertexTimes = step.isLoop() ?
              Lists.newArrayList(fromVertexId) :
              Lists.newArrayList(fromVertexId, entry.getToVertexId());

            List<Integer> edgesTimes = Lists.newArrayList(entry.getEdgeId());
            parentEmbeddings.add(new DFSEmbedding(vertexTimes, edgesTimes));
          }
        }
      }
      fromVertexId++;
    }

    while (iterator.hasNext() && !parentEmbeddings.isEmpty()) {
      Collection<DFSEmbedding> childEmbeddings = Lists.newArrayList();

      step = iterator.next();

      for (DFSEmbedding parentEmbedding : parentEmbeddings) {
        fromVertexId = parentEmbedding.getVertexTimes().get(step.getFromTime());

        for (AdjacencyListEntry entry :
          graph.getAdjacencyLists().get(fromVertexId).getEntries()) {

          // edge not contained
          if (!parentEmbedding.getEdgeTimes().contains(entry.getEdgeId())) {
            // forward traversal
            if (step.isForward() &&
              // same to vertex label
              step.getToLabel().equals(entry.getToVertexLabel()) &&
              // same edge label
              step.getEdgeLabel().equals(entry.getEdgeLabel()) &&
              // vertex not contained
              ! parentEmbedding.getVertexTimes()
                .contains(entry.getToVertexId())) {

              DFSEmbedding childEmbedding =
                DFSEmbedding.deepCopy(parentEmbedding);
              childEmbedding.getVertexTimes().add(entry.getToVertexId());
              childEmbedding.getEdgeTimes().add(entry.getEdgeId());

              childEmbeddings.add(childEmbedding);

              // backward traversal
            } else if (step.isBackward() &&
              // same edge label
              step.getEdgeLabel().equals(entry.getEdgeLabel()) &&
              // vertex already mapped to correct time
              parentEmbedding.getVertexTimes().get(step.getToTime())
                .equals(entry.getToVertexId())) {

              DFSEmbedding childEmbedding =
                DFSEmbedding.deepCopy(parentEmbedding);
              childEmbedding.getEdgeTimes().add(entry.getEdgeId());

              childEmbeddings.add(childEmbedding);
            }
          }
        }
      }

      parentEmbeddings = childEmbeddings;
    }

    return !parentEmbeddings.isEmpty();
  }

}
