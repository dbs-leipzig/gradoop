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
	public HashSet<RDBMSTable> cyclicTables;
	HashSet<String> visited;
	ArrayList<RDBMSTable> returnCyclicTables;

	public SequentialCycleFinder(ArrayList<RDBMSTable> tables) {
		this.tables = tables;
		this.cyclicTables = new HashSet<RDBMSTable>();
		this.cleanedTables = new ArrayList<RDBMSTable>();
		this.returnCyclicTables = new ArrayList<RDBMSTable>();
	}

	public HashSet<RDBMSTable> findAllCycles() {
		cleanTables();
		for (RDBMSTable table : cleanedTables) {
			visited = new HashSet<String>();
			Iterator it = table.getForeignKeys().entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Entry) it.next();
				findTableCycle(table, pair);
				// System.out.println(pair.getValue());
			}
		}
		return cyclicTables;
	}

	public void findTableCycle(RDBMSTable refingTable, Map.Entry<String, String> pair) {
		RDBMSTable referencing = refingTable;
		RDBMSTable observed = getRefdTable(pair.getValue());

		if (observed.getTableName().equals(referencing.getTableName())) {
			cyclicTables.add(observed);
		}

		if (!visited.contains(pair.getValue())) {
			visited.add(pair.getValue());
			for (RDBMSTable table : tables) {
				if (table.getTableName().equals(pair.getValue()) && table.getForeignKeys() != null) {
					Iterator it = table.getForeignKeys().entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry dkPair = (Entry) it.next();
						findTableCycle(table, pair);
					}
				}
			}

		}
	}
	
	public void cleanTables() {
		for(RDBMSTable table : tables){
			if(table.getForeignKeys() != null){
				cleanedTables.add(table);
			}
		}
	}
	
	public RDBMSTable getRefdTable(String refdTableStr){
		RDBMSTable refdTable = null;
		for(RDBMSTable table : tables){
			if(table.getTableName().equals(refdTable)){
				refdTable = table;
			}
		}
		return refdTable;
	}

	

}
