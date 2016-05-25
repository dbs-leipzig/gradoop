package org.gradoop.io.graphgen;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import java.util.Collection;
import java.util.HashSet;

/**
 * Created by stephan on 17.05.16.
 */
public class GraphGenReader {

  protected Collection<Tuple3<
    Long,
    Collection<Tuple2<Integer, String>>,
    Collection<Tuple3<Integer, Integer, String>>>> graphCollection;
  protected String content;

  /**
   * Creates a reader to get Collections from GraphGen files
   *
   */
  protected GraphGenReader() {
    graphCollection = new HashSet<>();

  }

  /**
   * Creates, or returns if already created, the collection of graphs as
   * tuples from content.
   *
   * @return Collection of graphs as tuples
   */
  public Collection<Tuple3<Long, Collection<Tuple2<Integer, String>>,
    Collection<Tuple3<Integer, Integer, String>>>> getGraphCollection() {

    if (content.isEmpty()) {
      return null;
    }
    if (!graphCollection.isEmpty()) {
      return graphCollection;
    }
    Long graphHead;
    Collection<Tuple2<Integer, String>> vertices = new HashSet<>();
    Collection<Tuple3< Integer, Integer, String>> edges = new HashSet<>();

    //remove first tag so that split does not have empty first element
    String[] contentArray = content.trim().replaceFirst("t # ", "").split("t " +
      "# ");

    for (int i = 0; i < contentArray.length; i++) {
      vertices.addAll(this.getVertices(contentArray[i]));
      edges.addAll(this.getEdges(contentArray[i]));
      graphHead = this.getGraphHead(contentArray[i]);

      graphCollection.add(new Tuple3<Long, Collection<Tuple2<Integer, String>>,
        Collection<Tuple3<Integer, Integer, String>>>(graphHead, vertices, edges));

      vertices = new HashSet<>();
      edges = new HashSet<>();
    }
    return graphCollection;
  }

  /**
   * Reads the vertices of a complete GraphGen graph segment.
   *
   * @param content the GraphGen graph segment
   * @return collection of vertices as tuples
   */
  protected Collection<Tuple2<Integer, String>> getVertices(String content) {
    Collection<Tuple2<Integer, String>> vertexCollection = new HashSet<>();
    String[] vertex = new String[3];

    //-1 cause before e is \n
    content = content.substring(content.indexOf("v"), content.indexOf("e")-1);
    String[] vertices = content.split("\n");

    for (int i = 0; i < vertices.length; i++) {
      vertex = vertices[i].split(" ");
      vertexCollection.add(new Tuple2<Integer, String>(Integer.parseInt
        (vertex[1].trim()), vertex[2].trim()));
    }
    return vertexCollection;
  }

  /**
   * Reads the edges of a complete GraphGen graph segment
   *
   * @param content the GraphGen graph segment
   * @return collection of edges as tuples
   */
  protected Collection<Tuple3< Integer, Integer, String>> getEdges(String
    content) {

    Collection<Tuple3< Integer, Integer, String>> edgeCollection = new
      HashSet<>();
    String[] edge = new String[4];

    content = content.substring(content.indexOf("e"), content.length());
    String[] edges = content.split("\n");

    for (int i = 0; i < edges.length; i++) {
      edge = edges[i].split(" ");
      edgeCollection.add(new Tuple3<Integer, Integer, String>(Integer.parseInt
        (edge[1].trim()), Integer.parseInt(edge[2].trim()), edge[3]));
    }
    return edgeCollection;
  }

  protected Long getGraphHead(String content) {
    return Long.parseLong(content.substring(0,1));
  }

  public void setContent(String content) {
    this.content = content;
  }
}
