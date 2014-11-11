package org.biiig.epg.store;

import org.biiig.epg.model.Graph;
import org.biiig.epg.model.Vertex;
import org.biiig.epg.store.exceptions.UnsupportedTypeException;

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
