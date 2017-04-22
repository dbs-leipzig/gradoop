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

package org.gradoop.examples.io.parsers.rawedges;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Extracts from a string just the numbers
 */
public class NumberTokenizer implements MapFunction<String, List<String>> {

  /**
   * Returns the string representation for the edge. Each edge is represented by source++destination
   */
  private String returned = "";

  /**
   * Parses a string retrieving some numbers
   * @param toread  String where to extract numbers
   * @return        List of the retrieved numbers
   */
  public List<String> tokenize(String toread) {
    StringBuilder sb = new StringBuilder();
    StreamTokenizer t = new StreamTokenizer(new StringReader(toread));
    t.resetSyntax();
    t.parseNumbers();
    ArrayList<String> toret = new ArrayList<>();
    try {
      while (t.nextToken() != StreamTokenizer.TT_EOF) {
        if (t.ttype == StreamTokenizer.TT_NUMBER) {
          toret.add(t.sval.trim());
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
  public List<String> map(String value) throws Exception {
    return tokenize(value);
  }
}
