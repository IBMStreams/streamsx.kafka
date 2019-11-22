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
package com.ibm.streamsx.kafka.operators;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.CheckpointContext;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.kafka.DataGovernanceUtil;
import com.ibm.streamsx.kafka.IGovernanceConstants;
import com.ibm.streamsx.kafka.KafkaOperatorException;
import com.ibm.streamsx.kafka.MsgFormatter;
import com.ibm.streamsx.kafka.SystemProperties;
import com.ibm.streamsx.kafka.ToolkitInfoReader;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

@Libraries({ "opt/downloaded/*", "impl/lib/*" })
public abstract class AbstractKafkaOperator extends AbstractOperator implements StateHandler {

    public static final String CLIENT_ID_PARAM = "clientId";
    public static final String USER_LIB_PARAM = "userLib";
    public static final String APP_CONFIG_NAME_PARAM = "appConfigName";
    public static final String PROPERTIES_FILE_PARAM = "propertiesFile";
    public static final String SSL_DEBUG_PARAM = "sslDebug";

    private static final Logger logger = Logger.getLogger(AbstractKafkaOperator.class);
    protected static final Level DEBUG_LEVEL = SystemProperties.getDebugLevelOverride();

    private static final String DEFAULT_USER_LIB_DIR = "/etc/libs/*"; //$NON-NLS-1$
    protected static final MetaType[] SUPPORTED_ATTR_TYPES = { 
            MetaType.RSTRING, MetaType.INT32, 
            MetaType.INT64, MetaType.UINT32, MetaType.UINT64,
            MetaType.FLOAT32, MetaType.FLOAT64, MetaType.BLOB 
    };

    protected String propertiesFile;
    protected String appConfigName;
    protected String[] userLib;
    protected String clientId = null;

    protected ConsistentRegionContext crContext;
    protected CheckpointContext chkptContext;
    private Boolean inParallelRegion = null;

    private KafkaOperatorProperties kafkaProperties;

    @Parameter(optional = true, name=PROPERTIES_FILE_PARAM, 
            description="Specifies the name of the properties file "
                    + "containing Kafka properties. A relative path is always "
                    + "interpreted as relative to the *application directory* of the "
                    + "Streams application.")
    public void setPropertiesFile(String propertiesFile) {
        this.propertiesFile = propertiesFile;
    }

    @Parameter(optional = true, name=APP_CONFIG_NAME_PARAM,
            description="Specifies the name of the application configuration "
                    + "containing Kafka properties.")
    public void setAppConfigName(String appConfigName) {
        this.appConfigName = appConfigName;
    }

    @Parameter(optional = true, name = SSL_DEBUG_PARAM,
            description = "If SSL/TLS protocol debugging is enabled, all SSL protocol data and information "
                    + "is logged to the console. This setting is equivalent to **vmArg: \\\"-Djavax.net.debug=true\\\";**. "
                    + "The default value for this parameter is `false`.\\n"
                    + "The parameter is ignored when the `javax.net.debug` property is set via the **vmArg** parameter.")
    public void setSslDebug (boolean sslDebug) {
        final String javaxNetDebug = System.getProperty ("javax.net.debug");
        if (sslDebug && javaxNetDebug == null) {
            System.setProperty ("javax.net.debug", "true");
        }
        if (javaxNetDebug != null || sslDebug) {
            System.out.println ("Property javax.net.debug: " + System.getProperty ("javax.net.debug"));
        }
    }

    @Parameter(optional = true, name=USER_LIB_PARAM,
            description="Allows the user to specify paths to JAR files that should "
                    + "be loaded into the operators classpath. This is useful if "
                    + "the user wants to be able to specify their own partitioners. "
                    + "The value of this parameter can either point to a specific JAR file, "
                    + "or to a directory. In the case of a directory, the operator will "
                    + "load all files ending in `.jar` onto the classpath. By default, "
                    + "this parameter will load all jar files found in `<application_dir>/etc/libs`.")
    public void setUserLib(String[] userLib) {
        this.userLib = userLib;
    }

    @Parameter(optional = true, name=CLIENT_ID_PARAM,
            description="Specifies the client ID that should be used "
                    + "when connecting to the Kafka cluster. The value "
                    + "specified by this parameter will override the `client.id` "
                    + "Kafka property if specified.\\n"
                    + "\\n"
                    + "Each operator must have a unique client ID. When operators "
                    + "are replicated by a parallel region, the channel-ID is automatically appended "
                    + "to the clientId to make the client-ID distinct for the parallel channels.\\n"
                    + "\\n"
                    + "If this parameter is not "
                    + "specified and the `client.id` Kafka property is not "
                    + "specified, the operator will create an ID with the pattern "
                    + "`C-J<job-ID>-<operator name>` for a consumer operator, and "
                    + "`P-J<job-ID>-<operator name>` for a producer operator.")
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Determines if the operator is used within a parallel region.
     * @return true, if the operator is used in a parallel region, false otherwise.
     * @throws KafkaOperatorException The operator is not yet initialized.
     */
    public boolean isInParallelRegion() throws KafkaOperatorException {
        if (inParallelRegion == null) throw new KafkaOperatorException ("operator must be initialized before");
        return inParallelRegion.booleanValue();
    }

    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
        try {
            ToolkitInfoReader tkr = new ToolkitInfoReader (context);
            logger.info ("Toolkit information: name = " + tkr.getToolkitName() + ", version = " + tkr.getToolkitVersion());
        }
        catch (Exception e) {
            logger.warn ("Could not determine toolkit name and version: " + e);
        }
        List<String> paramNames = new LinkedList<String> (context.getParameterNames());
        Collections.sort (paramNames);
        logger.info ("Used operator parameters: " + paramNames);
        this.inParallelRegion = new Boolean (context.getChannel() >= 0);
        crContext = context.getOptionalContext (ConsistentRegionContext.class);
        chkptContext = context.getOptionalContext (CheckpointContext.class);
        // load the Kafka properties
        kafkaProperties = new KafkaOperatorProperties();
        loadProperties();
        // set the client ID property if the clientId parameter is specified
        if(clientId != null && !clientId.isEmpty()) {
            kafkaProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }

        if (userLib == null) {
            userLib = new String[] { context.getPE().getApplicationDirectory() + DEFAULT_USER_LIB_DIR };
        } else {
            // convert all of the paths to absolute paths (if necessary)
            List<String> absLibPaths = new ArrayList<String>();
            for (String libPath : userLib)
                absLibPaths.add(convertToAbsolutePath(libPath).getAbsolutePath());
            userLib = absLibPaths.toArray(new String[0]);
        }
        logger.info("Loading user libraries: " + Arrays.asList(userLib)); //$NON-NLS-1$
        context.addClassLibraries(userLib);
    }

    protected void registerForDataGovernance(OperatorContext context, List<String> topics, boolean registerAsInput) {
        String opName = context.getLogicalName();
        logger.info(opName + " - Registering for data governance as " + (registerAsInput? "input/source": "output/sink")); //$NON-NLS-1$
        if (topics != null && topics.size() > 0) {
            for (String topic : topics) {
                logger.info(opName + " - data governance - topic: " + topic); //$NON-NLS-1$
                DataGovernanceUtil.registerForDataGovernance(this, topic, IGovernanceConstants.ASSET_KAFKA_TOPIC_TYPE,
                        null, null, registerAsInput, opName); // $NON-NLS-1$
            }
        } else {
            logger.info(opName + " - Registering for data governance -- topics is empty"); //$NON-NLS-1$
        }
    }

    protected void loadProperties() throws Exception {
        loadFromPropertiesFile();
        loadFromAppConfig();
    }

    protected Class<?> getAttributeType(StreamingData port, String attributeName) {
        return port.getStreamSchema().getAttribute(attributeName).getType().getObjectType();
    }

    protected void loadFromPropertiesFile() throws Exception {
        if (propertiesFile == null) {
            logger.info("No properties file specified"); //$NON-NLS-1$
            return;
        }
        File propFile = convertToAbsolutePath(propertiesFile);
        InputStream inStream = null;
        try {
            inStream = new FileInputStream (propFile);
            Properties props = new Properties();
            props.load (new InputStreamReader(inStream, Charset.forName ("UTF-8")));
            loadFromProperties (props);
        }
        catch (FileNotFoundException filenotfound) {
            logger.warn(Messages.getString ("PROPERTIES_FILE_NOT_FOUND", propFile.getAbsoluteFile())); //$NON-NLS-1$
            return;
        }
        catch (IOException e) {
            logger.error (Messages.getString ("PROPERTIES_FILE_NOT_READABLE", propFile.getAbsoluteFile(), e.getLocalizedMessage()));
            throw e;
        }
        finally {
            if (inStream != null) {
                try {
                    inStream.close();
                }
                catch (Exception e) {
                    logger.debug (e.getMessage());
                }
            }
        }
    }

    /**
     * traces the properties sorted according their keys at info level 
     * @param props
     */
    protected void tracePropsSorted (Properties props) {
        List<String> sortedKeys = new LinkedList<String>();
        for (Object k: props.keySet()) sortedKeys.add((String)k);
        sortedKeys.sort(new Comparator<String>() {
            @Override
            public int compare (String o1, String o2) {
                return o1.compareToIgnoreCase(o2);
            }
        });
        for (String key: sortedKeys) logger.info (MsgFormatter.format("{0} = {1}",  key, props.getProperty(key)));
    }

    protected void loadFromAppConfig() throws Exception {
        if (appConfigName == null)
            return;

        Map<String, String> appConfig = getOperatorContext().getPE().getApplicationConfiguration(appConfigName);
        if (appConfig.isEmpty()) {
            logger.warn(Messages.getString("APPLICATION_CONFIGURATION_NOT_FOUND", appConfigName)); //$NON-NLS-1$
            return;
        }

        Properties p = new Properties();
        appConfig.forEach((key, value) -> {
            p.put(key, value);
        });
        loadFromProperties(p);
    }

    protected void loadFromProperties(Properties properties) {
        kafkaProperties.putAllIfNotPresent(properties);
    }

    protected KafkaOperatorProperties getKafkaProperties() {
        return this.kafkaProperties;
    }

    /**
     * converts an attribute object to the Java primitive object
     * @param type    Not used
     * @param attrObj the attribute value as object
     * @return        The corresponding Java primitive
     */
    protected Object toJavaPrimitveObject(Class<?> type, Object attrObj) {
        if(attrObj instanceof RString) {
            attrObj = ((RString)attrObj).getString();
        } else if(attrObj instanceof Blob) {
            attrObj = ((Blob)attrObj).getData();
        }

        return attrObj;
    }

    protected File convertToAbsolutePath(String filePath) {
        File f = new File(filePath);
        if (!f.isAbsolute()) {
            File appDir = getOperatorContext().getPE().getApplicationDirectory();
            logger.info ("extending relative path '" + filePath + "' by the '" + appDir + "' directory");
            f = new File(appDir, filePath);
        }
        return f;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void retireCheckpoint(long id) throws Exception {
    }
}
