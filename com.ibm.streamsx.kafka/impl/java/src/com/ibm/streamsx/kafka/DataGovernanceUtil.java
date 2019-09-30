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

import java.util.HashMap;
import java.util.Map;

import com.ibm.streams.operator.AbstractOperator;

public class DataGovernanceUtil {

    public static void registerForDataGovernance(AbstractOperator operator, String assetName, String assetType, String parentAssetName, String parentAssetType, boolean isInput, String operatorType) {
        Map<String, String> properties = new HashMap<String, String>();        
        if(isInput) {
            properties.put(IGovernanceConstants.TAG_REGISTER_TYPE, IGovernanceConstants.TAG_REGISTER_TYPE_INPUT);
            properties.put(IGovernanceConstants.PROPERTY_INPUT_OPERATOR_TYPE, operatorType);
        } else {
            properties.put(IGovernanceConstants.TAG_REGISTER_TYPE, IGovernanceConstants.TAG_REGISTER_TYPE_OUTPUT);
            properties.put(IGovernanceConstants.PROPERTY_OUTPUT_OPERATOR_TYPE, operatorType);
        }
        properties.put(IGovernanceConstants.PROPERTY_SRC_NAME, assetName);
        properties.put(IGovernanceConstants.PROPERTY_SRC_TYPE, IGovernanceConstants.ASSET_STREAMS_PREFIX + assetType);
        if(parentAssetName != null) {
            properties.put(IGovernanceConstants.PROPERTY_SRC_PARENT_PREFIX, IGovernanceConstants.PROPERTY_PARENT_PREFIX);
            properties.put(IGovernanceConstants.PROPERTY_PARENT_PREFIX + IGovernanceConstants.PROPERTY_SRC_NAME, parentAssetName);
            properties.put(IGovernanceConstants.PROPERTY_PARENT_PREFIX + IGovernanceConstants.PROPERTY_SRC_TYPE, IGovernanceConstants.ASSET_STREAMS_PREFIX + parentAssetType);        
            properties.put(IGovernanceConstants.PROPERTY_PARENT_PREFIX + IGovernanceConstants.PROPERTY_PARENT_TYPE, "$" + parentAssetType); //$NON-NLS-1$
        }
        
        operator.setTagData(IGovernanceConstants.TAG_OPERATOR_IGC, properties);
    }
}