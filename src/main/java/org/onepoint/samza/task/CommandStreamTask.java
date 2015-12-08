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

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;


import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CommandStreamTask implements StreamTask, InitableTask {
    private String systemName = "kafka";
    private SystemStream OUTPUT_STREAM;
    private List<Integer> allPartitions = new ArrayList<Integer>();


  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

      Object o = envelope.getMessage();
      Object k = envelope.getKey();
      if(o instanceof byte[] && k instanceof byte[])
        try {
            String key = new String((byte[]) k, "UTF-8");
            String message = new String((byte[]) o, "UTF-8");
            sendMessage(collector, key, message);
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }
      else if(o instanceof String && k instanceof String) {
            sendMessage(collector, (String) k, (String) o);
          }
      else
        System.out.println("Message of unexpented type:" + o.getClass().getName());
  }

    private void sendMessage(MessageCollector collector, String key, String message) {
        if(allPartitions.isEmpty()) {
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, key, message));
        }
        else{
            for(Integer i: allPartitions) {
                collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, i, key, message));
            }
        }
    }


    @Override
  public void init(Config config, TaskContext tc) throws Exception {

      String kafka_hosts_ports  = config.get("systems."+systemName + ".producer.bootstrap.servers");
      String kafka_host_port = kafka_hosts_ports.split(",")[0];
      String topic  = config.get("task.output-stream");

      OUTPUT_STREAM = new SystemStream(systemName, config.get("task.output-stream"));

    //TODO: read config and configure pipeline
        if(kafka_host_port.contains(":")) {

            SimpleConsumer consumer = new SimpleConsumer(kafka_host_port.split(":")[0],
                    Integer.decode(kafka_host_port.split(":")[1]),
                    100000,
                    64 * 1024, "info-fetcher");
            List<String> topics = new ArrayList<String>();
            topics.add(topic);

            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
            System.out.println(resp.toString());
            for (TopicMetadata tm : resp.topicsMetadata()) {
                for (PartitionMetadata pm : tm.partitionsMetadata()) {
                    allPartitions.add(pm.partitionId());
                    System.out.println(pm.partitionId());
                }
            }
        }

  }
}

