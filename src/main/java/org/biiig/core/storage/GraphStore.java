package org.biiig.core.storage;

import org.biiig.core.model.Graph;
import org.biiig.core.model.Vertex;
import org.biiig.core.storage.exceptions.UnsupportedTypeException;

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
