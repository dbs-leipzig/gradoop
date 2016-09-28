package org.gradoop.flink.algorithms.fsm.common.canonicalization.gspan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Mapping {
  private final Map<Integer, Integer> vertexTimes;
  private final List<Integer> rightmostPath;
  private final Collection<Integer> edgeCoverage;

  public Mapping(Integer startVertex) {
    this.vertexTimes = Maps.newHashMap();
    this.vertexTimes.put(startVertex, 0);
    this.rightmostPath = Lists.newArrayList(startVertex);
    this.edgeCoverage = Sets.newHashSet();
  }

  private Mapping(Map<Integer, Integer> vertexTimes,
    List<Integer> rightmostPath, Collection<Integer> edgeCoverage) {

    this.vertexTimes = vertexTimes;
    this.rightmostPath = rightmostPath;
    this.edgeCoverage = edgeCoverage;
  }

  public int getRightmostVertexId() {
    return rightmostPath.get(rightmostPath.size() - 1);
  }

  public boolean containsEdge(int edgeId) {
    return edgeCoverage.contains(edgeId);
  }

  public List<Integer> getRightmostPath() {
    return rightmostPath;
  }

  public int getRole(int vertexId) {
    int role;

    if (vertexId == getRightmostVertexId()) {
      role = VertexRole.IS_RIGHTMOST;
    } else if (rightmostPath.contains(vertexId)) {
      role = VertexRole.ON_RIGHTMOST_PATH;
    } else if (vertexTimes.containsKey(vertexId)) {
      role = VertexRole.CONTAINED;
    } else {
      role = VertexRole.NOT_CONTAINED;
    }

    return role;
  }

  public Map<Integer, Integer> getVertexTimes() {
    return vertexTimes;
  }

  public Mapping growBackwards(Integer edgeId, int toId) {
    // keep vertex mapping
    Map<Integer, Integer> childVertexTimes = Maps.newHashMap(vertexTimes);

    // keep rightmost path until backtrace vertex and add rightmost
    List<Integer> childRightmostPath = Lists.newArrayList();
    for (int vertexId : rightmostPath) {
      childRightmostPath.add(vertexId);
      if (vertexId == toId) {
        break;
      }
    }
    childRightmostPath.add(getRightmostVertexId());

    // add new edge to coverage
    Collection<Integer> childEdgeCoverage = Sets.newHashSet(edgeCoverage);
    childEdgeCoverage.add(edgeId);

    return new Mapping(
      childVertexTimes, childRightmostPath, childEdgeCoverage);
  }

  public Mapping growForwards(int fromId, int edgeId, int toId) {
    // add new vertex to vertex mapping
    Map<Integer, Integer> childVertexTimes = Maps.newHashMap(vertexTimes);
    childVertexTimes.put(toId, vertexTimes.size());

    // keep rightmost path until start vertex and add new vertex
    List<Integer> childRightmostPath = Lists.newArrayList();
    for (int vertexId : rightmostPath) {
      childRightmostPath.add(vertexId);
      if (vertexId == fromId) {
        break;
      }
    }
    childRightmostPath.add(toId);

    // add new edge to coverage
    Collection<Integer> childEdgeCoverage = Sets.newHashSet(edgeCoverage);
    childEdgeCoverage.add(edgeId);

    return new Mapping(
      childVertexTimes, childRightmostPath, childEdgeCoverage);
  }

  @Override
  public String toString() {
    return vertexTimes + "\n" + rightmostPath + "\n" + edgeCoverage + "\n";
  }
}
