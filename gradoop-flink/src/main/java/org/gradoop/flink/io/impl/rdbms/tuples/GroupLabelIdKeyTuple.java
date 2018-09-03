package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

public class GroupLabelIdKeyTuple extends Tuple4<GradoopId,String,GradoopId,String>{
	private GradoopId groupId;
	private String label;
	private GradoopId vertexId;
	private String keyValue;
	
	public GroupLabelIdKeyTuple() {
	}

	public GroupLabelIdKeyTuple(GradoopId groupId, String label, GradoopId vertexId, String keyValue) {
		this.groupId = groupId;
		this.f0 = groupId;
		this.label = label;
		this.f1 = label;
		this.vertexId = vertexId;
		this.f2 = vertexId;
		this.keyValue = keyValue;
		this.f3 = keyValue;
	}

	public GradoopId getGroupId() {
		return groupId;
	}

	public void setGroupId(GradoopId groupId) {
		this.groupId = groupId;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public GradoopId getVertexId() {
		return vertexId;
	}

	public void setVertexId(GradoopId vertexId) {
		this.vertexId = vertexId;
	}

	public String getKeyValue() {
		return keyValue;
	}

	public void setKeyValue(String keyValue) {
		this.keyValue = keyValue;
	}
}
