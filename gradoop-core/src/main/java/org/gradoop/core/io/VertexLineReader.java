package org.gradoop.core.io;

import org.gradoop.core.model.Vertex;

/**
 * Created by s1ck on 11/11/14.
 */
public interface VertexLineReader {
  Vertex readLine(String line);
}
