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

package org.gradoop.flink.model.impl.nested.datarepresentation.functions;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Reading the binary format
 */
public class ReadGradoopIdGradoopIdSet extends FileInputFormat<Tuple2<GradoopId, GradoopId[]>> {

  private final byte[] gradoopIdArray;
  private final ArrayList<GradoopId> toreturn;
  private GradoopId array[];

  public ReadGradoopIdGradoopIdSet() {
    gradoopIdArray = new byte[GradoopId.ID_SIZE];
    toreturn = new ArrayList<>();
    array = new GradoopId[0];
  }

  @Override
  public boolean reachedEnd() throws IOException {
    try {
      return stream.available() > 0;
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public Tuple2<GradoopId, GradoopId[]> nextRecord(Tuple2<GradoopId, GradoopId[]> reuse) throws
    IOException {
    stream.read(); // ignoring the value
    stream.read(gradoopIdArray);
    reuse.f0 = GradoopId.fromByteArray(gradoopIdArray);
    int len = stream.read();
    boolean toUpdate = false;
    // Grow-only allocation policy
    if (len > array.length)
      toUpdate = true;
    toreturn.clear();
    for (int i = len; i > 0; i--) {
      stream.read(gradoopIdArray);
      toreturn.add(GradoopId.fromByteArray(gradoopIdArray));
    }
    if (toUpdate) {
      array = toreturn.toArray(array);
      reuse.f1 = array;
    } else {
      reuse.f1 = toreturn.toArray(array);
    }
    return reuse;
  }

}
