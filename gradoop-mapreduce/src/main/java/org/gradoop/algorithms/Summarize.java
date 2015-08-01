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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.algorithms;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.gradoop.GConstants;
import org.gradoop.io.formats.SummarizeWritable;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Summarize Operation
 */
public class Summarize {
  /**
   * Output: [graph-id, [vertex-id, (target1, target2, ...)]]
   */
  public static class SummarizeMapper extends
    TableMapper<LongWritable, SummarizeWritable> {

    /**
     * Converts vertices from/to HBase rows.
     */
    private VertexHandler vertexHandler;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setup(Context context) throws IOException,
      InterruptedException {
      Configuration conf = context.getConfiguration();
      Class<? extends VertexHandler> handlerClass = conf
        .getClass(GConstants.VERTEX_HANDLER_CLASS,
          GConstants.DEFAULT_VERTEX_HANDLER, VertexHandler.class);
      try {
        this.vertexHandler = handlerClass.getConstructor().newInstance();
      } catch (NoSuchMethodException | InstantiationException |
        IllegalAccessException | InvocationTargetException e) {
        e.printStackTrace();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
      Context context) throws IOException, InterruptedException {
      Vertex v = vertexHandler.readVertex(value);
      for (Long graph : vertexHandler.readGraphs(value)) {
        List<Edge> edges = Lists.newArrayList(v.getOutgoingEdges());
        context.write(new LongWritable(graph),
          new SummarizeWritable(v.getId(), edges));
      }
    }
  }

  /**
   * Input: graph-id, [[vertex-id, (target1, target2, ...)], ...]]
   * Output: graph-id [(graph-id, edge-count), ...]
   */
  public static class SummarizeReducer extends
    Reducer<LongWritable, SummarizeWritable, Text, Text> {

    /**
     * CRUD Access to graph store.
     */
    private GraphStore graphStore;
    /**
     * Converts graphs from/to HBase rows.
     */
    private GraphHandler graphHandler;
    /**
     * Converts vertices from/to HBase rows.
     */
    private VertexHandler vertexHandler;

    /**
     * Test OutputKey
     */
    private Text outputKey = new Text();

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setup(Context context) throws IOException,
      InterruptedException {
      Configuration conf = context.getConfiguration();
      Class<? extends GraphHandler> graphHandlerClass = conf
        .getClass(GConstants.GRAPH_HANDLER_CLASS,
          GConstants.DEFAULT_GRAPH_HANDLER, GraphHandler.class);
      Class<? extends VertexHandler> vertexHandlerClass = conf
        .getClass(GConstants.VERTEX_HANDLER_CLASS,
          GConstants.DEFAULT_VERTEX_HANDLER, VertexHandler.class);
      try {
        this.graphHandler = graphHandlerClass.getConstructor().newInstance();
        this.vertexHandler = vertexHandlerClass.getConstructor().newInstance();
        this.graphStore = HBaseGraphStoreFactory
          .createOrOpenGraphStore(conf, this.vertexHandler, this.graphHandler);
      } catch (NoSuchMethodException | InstantiationException |
        InvocationTargetException | IllegalAccessException e) {
        e.printStackTrace();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reduce(LongWritable key, Iterable<SummarizeWritable> values,
      Context context) throws IOException, InterruptedException {
      long communityKey = key.get();
      // Contains ID's of the community
      List<Long> community = new ArrayList<>();
      // Contains ID's of targets of edges of the community member
      List<Long> targets = new ArrayList<>();
      // Contains Community_ID's of edge targets that not belong to the
      // community
      List<Long> external = new ArrayList<>();
      for (SummarizeWritable idAndTargets : values) {
        community.add(idAndTargets.getVertexIdentifier());
        for (Long targetID : idAndTargets.getTargets()) {
          targets.add(targetID);
        }
      }
      Map<Long, Integer> communityCount = new HashMap<>();
      communityCount.put(communityKey, 0);
      for (Long target : targets) {
        if (!community.contains(target)) {
          Vertex v = graphStore.readVertex(target);
          for (Long externCommunityID : v.getGraphs()) {
            external.add(externCommunityID);
          }
        } else {
          communityCount
            .put(communityKey, communityCount.get(communityKey) + 1);
        }
      }
      //Counts edges with targets in other communities based on the communities
      // e.g. there are 3 edges to an other community with communityID 4
      // result communityCount<4,3>
      for (Long otherCommunity : external) {
        if (communityCount.containsKey(otherCommunity)) {
          communityCount
            .put(otherCommunity, communityCount.get(otherCommunity) + 1);
        } else {
          communityCount.put(otherCommunity, 1);
        }
      }
      /**
       *  CommunityID   for each <CommunityID,Count>
       *  communityKey  communityKey,communityCount.get(CommunityKey)
       */
      outputKey
        .set(writeCommunities(communityKey, community.size(), communityCount));
      context.write(outputKey, new Text(""));
    }

    /**
     * Builds TextKey output
     *
     * @param communityKey   Community Identifier
     * @param communitySize  Size of the Community
     * @param communityCount counts edges to the commintValueunity
     * @return string
     */
    private String writeCommunities(Long communityKey, int communitySize,
      Map<Long, Integer> communityCount) {
      StringBuilder sb = new StringBuilder();
      sb.append(communityKey);
      sb.append(" ");
      sb.append(communitySize);
      sb.append(" ");
      for (Map.Entry<Long, Integer> entryMap : communityCount.entrySet()) {
        sb.append(entryMap.getKey());
        sb.append(",");
        sb.append(entryMap.getValue());
        sb.append(" ");
      }
      return sb.toString();
    }
  }
}
