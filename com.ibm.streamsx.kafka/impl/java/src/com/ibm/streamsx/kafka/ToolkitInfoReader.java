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
package com.ibm.streamsx.kafka;

import java.io.FileInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.ibm.streams.operator.OperatorContext;

/**
 * Reads toolkit information from the toolkit.xml file of this toolkit.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public class ToolkitInfoReader {
    private String toolkitVersion = "unknown";
    private String toolkitName = "unknown";

    /**
     * @return the toolkit version
     */
    public String getToolkitVersion() {
        return toolkitVersion;
    }

    /**
     * @return the toolkit name
     */
    public String getToolkitName() {
        return toolkitName;
    }

    /**
     * Constructs a new ToolkitInfoReader and reads toolkit.xml of the toolkit, which declares the operator kind.
     * @param context The operator context
     * @throws Exception Reading toolkit.xml failed.
     */
    public ToolkitInfoReader (OperatorContext context) throws Exception {
        FileInputStream inStream = null;
        try {
            String toolkitXml = context.getToolkitDirectory().getPath() + "/toolkit.xml";
            inStream = new FileInputStream (toolkitXml);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse (inStream);
            doc.getDocumentElement().normalize();
            String rootNode =  doc.getDocumentElement().getNodeName();
            NodeList rootNodes = doc.getElementsByTagName(rootNode);
            if (rootNodes.getLength() == 1) {
                NodeList toolkitElems = ((Element) rootNodes.item(0)).getElementsByTagName("toolkit");
                if (toolkitElems.getLength() == 1) {
                    toolkitName = ((Element) toolkitElems.item(0)).getAttribute("name");
                    toolkitVersion = ((Element) toolkitElems.item(0)).getAttribute("version");
                }
            }
        }
        finally {
            if (inStream != null) {
                try {
                    inStream.close();
                }
                catch (Exception e) {
                    // ignore
                }
            }
        }
    }
}

