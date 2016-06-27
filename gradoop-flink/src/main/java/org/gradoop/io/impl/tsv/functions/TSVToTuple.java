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

package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;

import java.util.regex.Pattern;


/**
 * Reads vertex data from a tsv document and creates a Tuple6 Dataset. The
 * document contains at least the source vertex id, a language property of
 * the source vertex, a target vertex id and a language property of the
 * target vertex. All entries should be space separated.
 * <p>
 * Example:
 * <p>
 * 0 EN 1 ZH
 * 2 EN 3 JA
 * 4 EN 5 SV
 * 4 EN 6 SV
 * 7 EN 8 JA
 * 9 SV 8 JA
 */
public class TSVToTuple implements MapFunction<String,
  Tuple6<String, GradoopId, String, String, GradoopId, String>> {
  /**
   * Separator token
   */
  private static final Pattern LINE_SEPARATOR_TOKEN = Pattern.compile(" ");
  /**
   * Reuse Tuple
   */
  private Tuple6<String, GradoopId, String, String, GradoopId, String> reuse;

  /**
   * Constructor
   */
  public TSVToTuple() {
    this.reuse = new Tuple6<>();
  }

  /**
   * Reads every line of the tsv document and creates a tuple6 that contains
   * the two origin ids, generated gradoop ids and the language properties
   *
   * @param s           line of tsv document
   * @return            tuple6 containing origin ids, gradoopIds and properties
   * @throws Exception
   */
  @Override
  public Tuple6<String, GradoopId, String, String, GradoopId, String> map(
    String s) throws Exception {
    String[] token = LINE_SEPARATOR_TOKEN.split(s);
    reuse.f0 = token[0];        // origin id vertex 1
    reuse.f1 = GradoopId.get(); // generated GradoopId for vertex 1
    reuse.f2 = token[1];        // language property vertex 1
    reuse.f3 = token[2];        // origin id vertex 2
    reuse.f4 = GradoopId.get(); //  generated GradoopId for vertex 2
    reuse.f5 = token[3];        // language property vertex 2
    return reuse;
  }
}
