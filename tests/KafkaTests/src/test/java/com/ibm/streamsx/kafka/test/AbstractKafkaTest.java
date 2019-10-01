package com.ibm.streamsx.kafka.test;

import java.io.File;

import com.ibm.streamsx.kafka.test.utils.Constants;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.spl.SPL;

abstract class AbstractKafkaTest {

    private final Topology topology;
    private final String testName;

    public AbstractKafkaTest() throws Exception {
        this.testName = this.getClass().getName().replace(this.getClass().getPackage().getName() + ".", "");
        topology = createTopology (this.testName);
    }

    /**
     * creates a Topology object with added toolkit and property file as file dependency.
     * @param name the name of the topology
     * @return the topology instance
     * @throws Exception
     */
    protected Topology createTopology (String name) throws Exception {
        Topology t = new Topology(name);
        t.addFileDependency(Constants.PROPERTIES_FILE_PATH, "etc");
        SPL.addToolkit(t, new File("../../com.ibm.streamsx.kafka"));

        return t;
    }

    /**
     * Gets the name of the test case. This is part of the Main composite name.
     * @return the testName
     */
    public String getTestName() {
        return testName;
    }

    /**
     * Gets the Topology instance that is created for the test case
     * @return the Topology instance
     */
    public Topology getTopology() {
        return topology;
    }
}
