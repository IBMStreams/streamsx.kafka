/*
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
