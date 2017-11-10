/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.minifi;

import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;

import java.io.File;

import static org.mockito.Mockito.atLeastOnce;

public class MiNiFiTest {

    private static final String TEST_FLOW_XML_PATH = "./src/test/resources/flow.xml.gz";
    private static final File TEST_FLOW_XML_FILE = new File(TEST_FLOW_XML_PATH);

    @Test
    public void enrichFlowWithBundleInformationTest() throws Exception {
        final FlowParser mockFlowParser = Mockito.mock(FlowParser.class);

        // Create a copy of the document to use with our mock
        FlowParser parser = new FlowParser();
        Document flowDocument = parser.parse(TEST_FLOW_XML_FILE);
        Mockito.when(mockFlowParser.parse(Mockito.any())).thenReturn(flowDocument);

        final NiFiProperties mockProperties = Mockito.mock(NiFiProperties.class);
        Mockito.when(mockProperties.getFlowConfigurationFile()).thenReturn(new File(TEST_FLOW_XML_PATH));

        MiNiFi.enrichFlowWithBundleInformation(mockFlowParser, mockProperties);

        Mockito.verify(mockFlowParser, atLeastOnce()).writeFlow(Mockito.any(), Mockito.any());
    }

}
