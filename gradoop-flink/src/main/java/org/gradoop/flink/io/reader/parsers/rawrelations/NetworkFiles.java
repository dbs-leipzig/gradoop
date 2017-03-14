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

package org.gradoop.flink.io.reader.parsers.rawrelations;

import org.gradoop.flink.io.reader.parsers.rawedges.NumberTokenizer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Reading the network files format
 */
public class NetworkFiles {

  public static void main(String[] args) throws IOException {
    Map<Integer,Long> map = Files.lines(new File("/Volumes/Untitled/Data/SNAP-LiveJournal/" +
      "com-lj.all.cmty.txt").toPath())
      .filter(x -> !x.startsWith("#"))
      .map(x -> {
        NumberTokenizer<Double> nt = new NumberTokenizer<>(y -> y);
        return nt.tokenize(x).size();
      }).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    TreeMap<Integer,Long> tm = new TreeMap<>();
    tm.putAll(map);
    tm.forEach((k,v) -> { System.out.println(k+","+v); });
  }

}
