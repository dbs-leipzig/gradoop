/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.benchmark.nesting.serializers;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;

/**
 * Writes GradoopIds to bytes
 */
public class DeserializeGradoopidFromFile extends FileInputFormat<GradoopId> {

  /**
   * Reusable array
   */
  private final byte[] bytes;

  /**
   * Default constructor
   * @param filename File name to be read
   */
  public DeserializeGradoopidFromFile(Path filename) {
    super(filename);
    bytes = new byte[GradoopId.ID_SIZE];
  }

  public DeserializeGradoopidFromFile(String filename) {
    this(new Path(filename));
  }

  /**
   * Default constructor: no file name associated
   */
  public DeserializeGradoopidFromFile() {
    bytes = new byte[GradoopId.ID_SIZE];
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return stream.available() > 0;
  }

  @Override
  public GradoopId nextRecord(GradoopId gradoopId) throws IOException {
    if (stream.read(bytes) > 0) {
      return GradoopId.fromByteArray(bytes);
    } else {
      return null;
    }
  }

}
