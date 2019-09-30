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
package com.ibm.streamsx.kafka.clients.consumer;

/**
 * This interface defines pseudo offsets, which can be used to control
 * seeking the beginning or end of topic partitions.
 */
public interface OffsetConstants {
    public static final long SEEK_END = -1l;
    public static final long SEEK_BEGINNING = -2l;
    public static final long NO_SEEK = -3l;
    public static final long HIGHEST_VALID_OFFSET_VALUE = NO_SEEK;
}
