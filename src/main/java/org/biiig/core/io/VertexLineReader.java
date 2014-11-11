package org.biiig.core.io;

import org.biiig.core.model.Vertex;

/**
 * Created by s1ck on 11/11/14.
 */
public interface VertexLineReader {
  Vertex readLine(String line);
}
