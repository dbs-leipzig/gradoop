package org.gradoop.flink.algorithms.fsm.common.canonicalization.pojos;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.flink.algorithms.fsm.common.canonicalization.VertexRole;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DFSEmbedding {
  private final Map<Integer, Integer> vertexTimes;
  private final List<Integer> rightmostPath;
  private final Collection<Integer> edgeCoverage;

  public DFSEmbedding(Integer startVertex) {
    this.vertexTimes = Maps.newHashMap();
    this.vertexTimes.put(startVertex, 0);
    this.rightmostPath = Lists.newArrayList(startVertex);
    this.edgeCoverage = Sets.newHashSet();
  }

  private DFSEmbedding(Map<Integer, Integer> vertexTimes,
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

    if (vertexTimes.containsKey(vertexId)) {
      role = VertexRole.CONTAINED;

      if (rightmostPath.contains(vertexId)) {
        role = VertexRole.ON_RIGHTMOST_PATH;

        if (vertexId == getRightmostVertexId()) {
          role = VertexRole.IS_RIGHTMOST;
        }
      }
    } else {
      role = VertexRole.NOT_CONTAINED;
    }

    return role;
  }

  public Map<Integer, Integer> getVertexTimes() {
    return vertexTimes;
  }

  public DFSEmbedding growBackwards(int toTime, Integer edgeId) {
    Map<Integer, Integer> childVertexTimes = Maps.newHashMap(vertexTimes);

    List<Integer> childRightmostPath = Lists.newArrayList();

    int time = 0;
    Iterator<Integer> timeIterator = childRightmostPath.iterator();

    while (time <= toTime) {
      childRightmostPath.add(timeIterator.next());
      time++;
    }

    childRightmostPath.add(getRightmostVertexId());

    Collection<Integer> childEdgeCoverage = Sets.newHashSet(edgeCoverage);
    childEdgeCoverage.add(edgeId);

    return new DFSEmbedding(
      childVertexTimes, childRightmostPath, childEdgeCoverage);
  }

  public DFSEmbedding growForwards(int fromTime, int edgeId, int toId) {
    Map<Integer, Integer> childVertexTimes = Maps.newHashMap(vertexTimes);
    childVertexTimes.put(toId, vertexTimes.size());

    List<Integer> childRightmostPath = Lists.newArrayList();

    int time = 0;
    Iterator<Integer> timeIterator = childRightmostPath.iterator();

    while (time <= fromTime) {
      childRightmostPath.add(timeIterator.next());
      time++;
    }

    childRightmostPath.add(toId);

    Collection<Integer> childEdgeCoverage = Sets.newHashSet(edgeCoverage);
    childEdgeCoverage.add(edgeId);

    return new DFSEmbedding(
      childVertexTimes, childRightmostPath, childEdgeCoverage);  }
}
