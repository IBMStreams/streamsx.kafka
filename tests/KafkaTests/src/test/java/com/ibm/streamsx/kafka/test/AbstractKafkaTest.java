package com.ibm.streamsx.kafka.test;

import java.io.File;

import com.ibm.streamsx.kafka.test.utils.Constants;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.spl.SPL;

abstract class AbstractKafkaTest {

	private Topology topo;
	
	public AbstractKafkaTest(String testName) throws Exception {
		topo = createTopology(testName);
	}
	
	protected Topology createTopology(String testName) throws Exception {
		Topology t = new Topology(testName);
		t.addFileDependency(Constants.PROPERTIES_FILE_PATH, "etc");
		SPL.addToolkit(t, new File("../../com.ibm.streamsx.kafka"));
		
		return t;
	}
	
	public Topology getTopology() {
		return topo;
	}
}
