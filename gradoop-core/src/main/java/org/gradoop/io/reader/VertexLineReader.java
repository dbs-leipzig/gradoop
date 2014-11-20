package org.gradoop.io.reader;

import org.gradoop.model.Vertex;

/**
 * Created by s1ck on 11/11/14.
 */
public interface VertexLineReader {
  Vertex readLine(String line);
}
