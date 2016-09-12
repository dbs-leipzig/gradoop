package org.gradoop.flink.algorithms.fsm;

import com.google.common.collect.Maps;
import org.gradoop.flink.algorithms.fsm.cam.EdgeTriple;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.intersection;

public class Embedding {

  private final Map<Integer, String> vertices;
  private final Map<Integer, EdgeTriple> edges;

  public Embedding(
    Map<Integer, String> vertices, Map<Integer, EdgeTriple> edges) {

    this.vertices = vertices;
    this.edges = edges;
  }

  public Map<Integer, String> getVertices() {
    return vertices;
  }


  @Override
  public String toString() {
    return vertices.toString() + "|" + edges.toString();
  }

  public Map<Integer, EdgeTriple> getEdges() {
    return edges;
  }

  public Set<Integer> getEdgeIds() {
    return edges.keySet();
  }

  public boolean sharesVerticesWith(Embedding that) {
    return
      ! intersection(this.vertices.keySet(), that.vertices.keySet()).isEmpty();
  }

  public boolean sharesEdgesWith(Embedding that) {
    return
      ! intersection(this.edges.keySet(), that.edges.keySet()).isEmpty();
  }

  public Embedding combine(Embedding that) {

    Map<Integer, String> commonVertices = Maps.newHashMap(vertices);
    commonVertices.putAll(that.vertices);

    Map<Integer, EdgeTriple> commonEdges =
      Maps.newHashMapWithExpectedSize(this.edges.size() + that.edges.size());

    commonEdges.putAll(this.edges);
    commonEdges.putAll(that.edges);

    return new Embedding(commonVertices, commonEdges);
  }

  public boolean sharesExactlyOneEdgeWith(Embedding that) {
    return intersection(this.edges.keySet(), that.edges.keySet()).size() == 1;
  }
}
