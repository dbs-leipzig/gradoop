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

package org.gradoop.flink.io.reader.parsers.rawedges;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Extracts from a string just the numbers
 *
 * @param <Format> The resulting desired format type
 */
public class NumberTokenizer<Format> implements MapFunction<String,List<Format>> {

  /**
   * Function converting doubles to the desired format
   */
  private final Function<Double, Format> converter;

  /**
   * Returns the string representation for the edge. Each edge is represented by source++destination
   */
  private String returned = "";

  /**
   * Default constructor
   * @param converter Function converting doubles to the desired format
   */
  public NumberTokenizer(Function<Double, Format> converter) {
    this.converter = converter;
  }


  /**
   * Parses a string retrieving some numbers
   * @param toread  String where to extract numbers
   * @return        List of the retrieved numbers
   */
  public List<Format> tokenize(String toread) {
    StringBuilder sb = new StringBuilder();
    StreamTokenizer t = new StreamTokenizer(new StringReader(toread));
    t.resetSyntax();
    t.parseNumbers();
    ArrayList<Format> toret = new ArrayList<>();
    try {
      while (t.nextToken() != StreamTokenizer.TT_EOF) {
        if (t.ttype == StreamTokenizer.TT_NUMBER) {
          toret.add(converter.apply(t.nval));
          sb.append(t.sval);
        }
      }
    } catch (IOException e) {
      return new ArrayList<>();
    }
    returned = sb.toString();
    return toret;
  }

  /**
   * Returns…
   * @return   the combined strings identifying the edge
   */
  public String getReturned() {
    return returned;
  }

  @Override
  public List<Format> map(String value) throws Exception {
    return tokenize(value);
  }
}
