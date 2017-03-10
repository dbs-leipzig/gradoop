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

package org.gradoop.flink.io.reader.parsers.amazon.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.flink.io.reader.parsers.amazon.AmazonAttributes;
import org.gradoop.flink.io.reader.parsers.amazon.edges.Reviews;
import org.gradoop.flink.io.reader.parsers.amazon.vertices.Item;
import org.gradoop.flink.io.reader.parsers.amazon.vertices.Reviewer;

import java.util.Iterator;

/**
 * Implementing the default parser of each json object string into an element
 */
public class MapAmazonEntryToTriple implements MapFunction<String, Tuple3<Reviewer, Reviews, Item>> {

  /**
   * Triple representing user (source), the review (edge), and item reviewed by the user (target)
   */
  private final Tuple3<Reviewer, Reviews, Item> reusableTriple;

  /**
   * Default constructor
   */
  public MapAmazonEntryToTriple() {
    reusableTriple = new Tuple3<>();
    reusableTriple.f0 = new Reviewer();
    reusableTriple.f2 = new Item();
    reusableTriple.f1 = new Reviews();
  }

  @Override
  public Tuple3<Reviewer, Reviews, Item> map(String value) throws Exception {
    JSONObject object = new JSONObject(value);

    //Return the Reviewer
    String id = "R" + object.getString(AmazonAttributes.reviewerID.value());
    String name = "anonymous";
    try {
      name = object.getString(AmazonAttributes.reviewerName.value());
    } catch (JSONException e) {
    }
    reusableTriple.f0.setRewId(id);
    reusableTriple.f0.setRewName(name);

    //Return the Item
    String iid = "I" + object.getString(AmazonAttributes.asin.value());
    reusableTriple.f2.setId(iid);

    //Create the edge
    reusableTriple.f1.setSrc(id);
    reusableTriple.f1.setDst(iid);
    Iterator keys = object.keys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      switch (AmazonAttributes.valueOf(key)) {
      case helpful:
        JSONArray arr = object.getJSONArray(key);
        Double num = arr.getDouble(0);
        Double den = arr.getDouble(1);
        reusableTriple.f1.set("helpfulness", num / den);
        break;
      case reviewText:
        reusableTriple.f1.set("text", object.getString(key));
        break;
      case overall:
        reusableTriple.f1.set("overall", object.getDouble(key));
        break;
      case summary:
        reusableTriple.f1.set("summary", object.getString(key));
        break;
      case unixReviewTime:
        reusableTriple.f1.set("time", object.getLong(key));
        break;
      case reviewTime:
        reusableTriple.f1.set("humanTime", object.getString(key));
        break;
      default:
        break;
      }
    }
    return reusableTriple;
  }
}
