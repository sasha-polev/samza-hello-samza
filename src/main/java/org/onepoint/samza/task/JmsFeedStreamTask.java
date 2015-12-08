/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.onepoint.samza.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


import java.io.IOException;
import java.util.Map;

/**
 * This task is very simple. All it does is take messages that it receives, and
 * sends them to a Kafka topic called wikipedia-raw.
 */
public class JmsFeedStreamTask implements StreamTask, InitableTask {

  private SystemStream OUTPUT_STREAM;
  private static final Log log = LogFactory.getLog(JmsFeedStreamTask.class);
  JsonFactory factory = new JsonFactory();
  ObjectMapper mapper = new ObjectMapper(factory);

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

    System.out.println("Processing message: " + envelope.getMessage());
    if(envelope.getMessage() instanceof String && ((String) envelope.getMessage()).startsWith("command"))
    {
      String message = (String) envelope.getMessage();
      String commandKey = message.split("\\|")[0];
      String payload = message.replace(commandKey + "|", "");

      TypeReference<Map<String,String>> typeRef  = new TypeReference<Map<String,String>>() {};

      try {
        //System.out.println("Trying to read payload: " + payload);
        Map<String,String> deserializedPayload = mapper.readValue(payload, typeRef);
        OutgoingMessageEnvelope outEnvelope = new OutgoingMessageEnvelope(OUTPUT_STREAM, null, commandKey, deserializedPayload);
        System.out.println("Outgoing message: " + outEnvelope);
        collector.send(outEnvelope);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    else if(envelope.getMessage() instanceof String) {

      //Guess event

      TypeReference<Map<String,Object>> typeRef  = new TypeReference<Map<String,Object>>() {};
      try {
        Map<String,String> deserializedPayload = mapper.readValue((String)envelope.getMessage(), typeRef);
        Object key = null;
        if(deserializedPayload.containsKey("Key"))
          key = deserializedPayload.get("Key");
        OutgoingMessageEnvelope outEnvelope = new OutgoingMessageEnvelope(OUTPUT_STREAM, null, key, deserializedPayload);
        System.out.println("Outgoing message: " + outEnvelope);
        collector.send(outEnvelope);
      } catch (IOException e) {
        e.printStackTrace();
      }


    }

  }



  @Override
  public void init(Config config, TaskContext tc) throws Exception {
      OUTPUT_STREAM = new SystemStream("kafka", config.get("task.output-stream"));


  }
}

