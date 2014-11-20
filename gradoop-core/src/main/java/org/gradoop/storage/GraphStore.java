package org.gradoop.storage;

import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.storage.exceptions.UnsupportedTypeException;

/**
 * Created by martin on 05.11.14.
 */
public interface GraphStore {
  void writeGraph(final Graph graph);

  void writeVertex(final Vertex vertex)
    throws UnsupportedTypeException;

  Graph readGraph(final Long graphID);

  Vertex readVertex(final Long vertexID);

  void close();
}
