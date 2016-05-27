package org.gradoop.model.impl.operators.matching.isomorphism.query;

import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Implementation of a Traversal, using a depth-first search.
 */
public class DFSTraversal implements Traversal {

  /**
   * List of the steps in this traversal.
   */
  private List<Step> steps;

  /**
   * Creates a new DFSTraversal from collections of vertices and edges.
   * @param vertices collection of vertices
   * @param edges collection of edges
   */
  private DFSTraversal(Collection<Vertex> vertices, Collection<Edge> edges) {
    Map<Long, List<Edge>> vertexIdToEdges = new HashMap<>();
    Map<Long, Boolean> vertexVisited = new HashMap<>();
    Map<Long, Boolean> edgeTraversed = new HashMap<>();
    steps = new ArrayList<>();
    for(Vertex vertex : vertices) {
      vertexIdToEdges.put(vertex.getId(), new ArrayList<Edge>());
    }
    for(Edge edge : edges){
      vertexIdToEdges.get(edge.getSourceVertexId()).add(edge);
      vertexIdToEdges.get(edge.getTargetVertexId()).add(edge);
    }

    Long currentVertexId = vertices.iterator().next().getId();
    vertexVisited.put(currentVertexId, true);
    Stack<Edge> edgeStack = new Stack<>();
    edgeStack.addAll(vertexIdToEdges.get(currentVertexId));
    while(edgeStack.size() > 0) {
      Edge edge = edgeStack.pop();
      if(!edgeTraversed.containsKey(edge.getId())){
        Long sourceId = edge.getSourceVertexId();
        Long targetId = edge.getTargetVertexId();
        Boolean sourceVisited = vertexVisited.containsKey(sourceId);
        Boolean targetVisited = vertexVisited.containsKey(targetId);
        if(sourceVisited && !targetVisited) {
          currentVertexId = targetId;
          steps.add(new Step(sourceId, edge.getId(), targetId, true ));
          edgeStack.addAll(vertexIdToEdges.get(currentVertexId));
          vertexVisited.put(currentVertexId, true);
        }
        if(!sourceVisited && targetVisited) {
          currentVertexId = sourceId;
          steps.add(new Step(sourceId, edge.getId(), targetId, false ));
          edgeStack.addAll(vertexIdToEdges.get(currentVertexId));
          vertexVisited.put(currentVertexId, true);
        }
        if(sourceVisited && targetVisited) {
          steps.add(new Step(sourceId, edge.getId(), targetId, true));
        }
        vertexVisited.put(currentVertexId, true);
        edgeTraversed.put(edge.getId(), true);
      }
    }
  }

  /**
   * Creates a new DFSTraversal from collections of vertices and edges.
   * @param vertices collection of vertices
   * @param edges collection of edges
   * @return new DFSTraversal
   */
  public static DFSTraversal createDFSTraversal(
    Collection<Vertex> vertices, Collection<Edge> edges) {
    return new DFSTraversal(vertices, edges);
  }

  /**
   * {@inheritDoc}
   */
  public List<Step> getSteps() {
    return steps;
  }

  /**
   * {@inheritDoc}
   */
  public Step getStep(int i) {
    return steps.get(i);
  }
}
