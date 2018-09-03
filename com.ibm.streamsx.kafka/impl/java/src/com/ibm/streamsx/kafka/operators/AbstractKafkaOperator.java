package com.ibm.streamsx.kafka.operators;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

import com.google.common.io.Files;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.kafka.DataGovernanceUtil;
import com.ibm.streamsx.kafka.IGovernanceConstants;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

@Libraries({ "opt/downloaded/*", "impl/lib/*" })
public abstract class AbstractKafkaOperator extends AbstractOperator implements StateHandler {

    private static final Logger logger = Logger.getLogger(AbstractKafkaOperator.class);

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

    protected Class<?> messageType;
    protected Class<?> keyType;
    protected ConsistentRegionContext crContext;

    private KafkaOperatorProperties kafkaProperties;

    @Parameter(optional = true, name="propertiesFile", 
    		description="Specifies the name of the properties file "
    				+ "containing Kafka properties. A relative path is always "
    				+ "interpreted as relative to the *application directory* of the "
    				+ "Streams application.")
    public void setPropertiesFile(String propertiesFile) {
        this.propertiesFile = propertiesFile;
    }

    @Parameter(optional = true, name="appConfigName",
    		description="Specifies the name of the application configuration "
    				+ "containing Kafka properties.")
    public void setAppConfigName(String appConfigName) {
        this.appConfigName = appConfigName;
    }

    @Parameter(optional = true, name="userLib",
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
    
    @Parameter(optional = true, name="clientId",
            description="Specifies the client ID that should be used "
                    + "when connecting to the Kafka cluster. The value "
                    + "specified by this parameter will override the `client.id` "
                    + "Kafka property if specified. If this parameter is not "
                    + "specified and the `client.id` Kafka property is not "
                    + "specified, the operator will use a random client ID.")
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
        
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
        if(!propFile.exists()) {
        	logger.warn(Messages.getString("PROPERTIES_FILE_NOT_FOUND", propFile.getAbsoluteFile())); //$NON-NLS-1$
        	return;
        }
        
        String propertyContent = Files.toString(propFile, StandardCharsets.UTF_8);
        if (propertyContent != null) {
            Properties props = new Properties();
            props.load(new StringReader(propertyContent));
            loadFromProperties(props);
        }
    }

    protected void loadFromAppConfig() throws Exception {
        if (appConfigName == null)
            return;

        Map<String, String> appConfig = getOperatorContext().getPE().getApplicationConfiguration(appConfigName);
        if (appConfig.isEmpty()) {
            logger.warn(Messages.getString("APPLICATION_CONFIG_NOT_FOUND", appConfigName)); //$NON-NLS-1$
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
            f = new File(getOperatorContext().getPE().getApplicationDirectory(), filePath);
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
