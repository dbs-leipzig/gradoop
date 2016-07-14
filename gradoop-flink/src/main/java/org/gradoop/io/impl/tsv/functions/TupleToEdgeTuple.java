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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Class to create a new DataSet that contains source and target id
 */
public class TupleToEdgeTuple
  implements MapFunction<Tuple4<Long, String, Long, String>,
  Tuple2<Long, Long>> {

  /**
   * reuse Tuple
   */
  private Tuple2<Long, Long> reuse;

  /**
   * Constructor
   */
  public TupleToEdgeTuple() {
    this.reuse = new Tuple2<>();
  }

  /**
   * Returns Tuple2 that contains source and target ids
   *
   * @param inputTuple information of one line of the tsv file
   * @return Tuple2
   * @throws Exception
   */
  @Override
  public Tuple2<Long, Long> map(Tuple4<Long, String, Long, String> inputTuple)
      throws Exception {
    reuse.f0 = inputTuple.f0;
    reuse.f1 = inputTuple.f2;
    return reuse;
  }
}
