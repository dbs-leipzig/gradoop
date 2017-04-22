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

package org.gradoop.examples.io.parsers.amazon;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.examples.io.parsers.AdjacencyListFileParser;
import org.gradoop.examples.io.parsers.GraphClob;
import org.gradoop.examples.io.parsers.amazon.edges.Reviews;
import org.gradoop.examples.io.parsers.amazon.functions.FromItemToVertex;
import org.gradoop.examples.io.parsers.amazon.functions.FromReviewerToVertex;
import org.gradoop.examples.io.parsers.amazon.functions.FromReviewsToEdge;
import org.gradoop.examples.io.parsers.amazon.functions.MapAmazonEntryToTriple;
import org.gradoop.examples.io.parsers.amazon.vertices.Item;
import org.gradoop.examples.io.parsers.amazon.vertices.Reviewer;
import org.gradoop.examples.io.parsers.functions.ImportVertexId;
import org.gradoop.examples.io.parsers.memetracker.MemeTrackerEdge;
import org.gradoop.examples.io.parsers.memetracker.MemeTrackerRecordParser;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of3;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of3;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;

/**
 * Implements the AdjacencyListFileParser with the default configurations
 */
public class AmazonFileParser extends AdjacencyListFileParser<String, MemeTrackerEdge, MemeTrackerRecordParser> {
  /**
   * Default constructor
   */
  public AmazonFileParser() {
    super(null, null, null);
    super.splitWith("\n\n");
  }

  @Override
  public GraphClob<String> asGeneralGraphDataSource() {
    DataSet<Tuple3<Reviewer, Reviews, Item>> coll = super.getDataset(new MapAmazonEntryToTriple());
    DataSet<ImportVertex<String>> reviewers = coll.map(new Value0Of3<>())
      .map(new FromReviewerToVertex())
      .distinct(new ImportVertexId<>());
    DataSet<ImportVertex<String>> items = coll.map(new Value2Of3<>())
      .map(new FromItemToVertex())
      .distinct(new ImportVertexId<>());
    DataSet<ImportVertex<String>> vertexDataSet = reviewers
      .union(items);
    DataSet<ImportEdge<String>> edgeDataSet = coll.map(new Value1Of3<>())
      .map(new FromReviewsToEdge());
    return new GraphClob<>(vertexDataSet, edgeDataSet);
  }
}
