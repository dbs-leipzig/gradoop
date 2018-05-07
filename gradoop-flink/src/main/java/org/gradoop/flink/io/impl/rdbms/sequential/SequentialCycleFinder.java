package org.gradoop.flink.io.impl.rdbms.sequential;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;

public class SequentialCycleFinder {
	ArrayList<RDBMSTable> tables;
	ArrayList<RDBMSTable> cleanedTables;
	public HashSet<String> cyclicTables;
	HashSet<String> visited;

	public SequentialCycleFinder(ArrayList<RDBMSTable> tables) {
		this.tables = tables;
		this.cyclicTables = new HashSet<String>();
		this.cleanedTables = new ArrayList<RDBMSTable>();
	}

	public HashSet<String> findAllCycles() {
		cleanTables();
		for (RDBMSTable table : cleanedTables) {
			visited = new HashSet<String>();
			Iterator it = table.getForeignKeys().entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Entry) it.next();
				findTableCycle(table.getTableName(), pair);
				// System.out.println(pair.getValue());
			}
		}
		return cyclicTables;
	}

	public void findTableCycle(String tableName, Map.Entry<String, String> pair) {
		String referencing = tableName;
		String observed = pair.getValue();

		if (observed.equals(referencing)) {
			cyclicTables.add(observed);
		}

		if (!visited.contains(pair.getValue())) {
			visited.add(pair.getValue());
			for (RDBMSTable table : tables) {
				if (table.getTableName().equals(pair.getValue()) && table.getForeignKeys() != null) {
					Iterator it = table.getForeignKeys().entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry dkPair = (Entry) it.next();
						findTableCycle(table.getTableName(), pair);
					}
				}
			}

		}
	}

	public void cleanTables() {
		for(RDBMSTable table : tables){
			if(table.getForeignKeys() != null){
				cleanedTables.add(table);
			}else{
				System.out.println();
			}
		}
	}

}
