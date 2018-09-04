package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Tuple used to represent primary key respectively foreign key site of foreign key relation
 *
 */
public class LabelIdKeyTuple extends Tuple3<String, GradoopId, String>{
	private String label;
	private GradoopId id;
	private String key;
	
	public LabelIdKeyTuple() {
	}
	
	public LabelIdKeyTuple(String label, GradoopId id, String key) {
		this.label = label;
		this.f0 = label;
		this.id = id;
		this.f1 = id;
		this.key = key;
		this.f2 = key;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public GradoopId getId() {
		return id;
	}

	public void setId(GradoopId id) {
		this.id = id;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
}
