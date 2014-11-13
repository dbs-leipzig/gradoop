package org.gradoop.core.storage;

import org.gradoop.core.model.Graph;
import org.gradoop.core.model.Vertex;
import org.gradoop.core.storage.exceptions.UnsupportedTypeException;

/**
 * Created by martin on 05.11.14.
 */
public interface GraphStore {
  void writeGraph(final Graph graph);

  void writeVertex(final Vertex vertex) throws UnsupportedTypeException;

  Graph readGraph(final Long graphID);

  Vertex readVertex(final Long vertexID);

  void close();
}
