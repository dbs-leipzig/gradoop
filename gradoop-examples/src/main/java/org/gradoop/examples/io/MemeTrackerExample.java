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

package org.gradoop.examples.io;

import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.examples.io.parsers.GraphClob;
import org.gradoop.examples.io.parsers.memetracker.MemeTrackerFileParser;

import java.io.File;

/**
 * Utility Class showing how to parse the MemeTracker folder
 */
public class MemeTrackerExample {

  /**
   * Path containing all the source files for the MemeTracker
   */
  private final String folderPath;

  /**
   * Default parser
   */
  private MemeTrackerFileParser defaulter;

  /**
   * Reads the MemeTracker graph and replace them
   * @param folderPath  Folder where
   */
  public MemeTrackerExample(String folderPath) {
    this.folderPath = folderPath;
    defaulter = new MemeTrackerFileParser();
  }

  /**
   * Read all the files within the collection as a single LogicalGraph
   * @return  Parse all the files and return them as a single graph.
   */
  public GraphDataSource<String> asGraphDataSourcesCollection() {
    File dir = new File(folderPath);
    File[] files = dir.listFiles((dir1, name) ->
      name.toLowerCase().endsWith(".txt") && name.toLowerCase().startsWith("quotes_"));
    GraphClob<String> clob = null;
    if (files == null) {
      return null;
    } else {
      for (File file : files) {
        GraphClob<String> x = defaulter.
          <MemeTrackerFileParser>fromFile(file.toString()).asGeneralGraphDataSource();
        if (clob == null) {
          clob = x;
        } else {
          clob.addAll(x);
        }
      }
      return clob != null ? clob.asGraphDataSource() : null;
    }
  }

  /**
   * Example showing how to read a single file
   * @param args  Sys.env arguments
   */
  public static void main(String[] args) {
    MemeTrackerExample mte = new MemeTrackerExample("/Volumes/Untitled/Data/Meme Tracker/");
    mte.asGraphDataSourcesCollection().getLogicalGraph(); // lg
  }

}
