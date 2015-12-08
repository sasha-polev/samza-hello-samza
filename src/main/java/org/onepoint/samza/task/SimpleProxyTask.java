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

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class SimpleProxyTask implements StreamTask, InitableTask {
  private SystemStream OUTPUT_STREAM;


  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

      Object o = envelope.getMessage();
      Object k = envelope.getKey();
      if(o instanceof byte[] && k instanceof byte[])
        try {
          collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, new String((byte[])k, "UTF-8"),  new String((byte[])o, "UTF-8"))); //new OutgoingMessageEnvelope(OUTPUT_STREAM, i, null, outgoingMap)
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }
      else if(o instanceof String && k instanceof String)
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, (String) k, (String) o)); //new OutgoingMessageEnvelope(OUTPUT_STREAM, i, null, outgoingMap)
      else
        System.out.println("Message of unexpented type:" + o.getClass().getName());
  }

  private List<Integer> allPartitions = new ArrayList<Integer>();

  @Override
  public void init(Config config, TaskContext tc) throws Exception {

      OUTPUT_STREAM = new SystemStream("kafka", config.get("task.output-stream"));

    //TODO: read config and configure pipeline

//    SimpleConsumer consumer  = new SimpleConsumer("127.0.0.1",
//            9092,
//            100000,
//            64 * 1024, "info-fetcher");
//    List<String> topics = new ArrayList<String>();
//    topics.add(WIKIPEDIA_RAW);
//    TopicMetadataRequest req = new TopicMetadataRequest(topics);
//    kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
//    System.out.println(resp.toString());
//    for(TopicMetadata tm : resp.topicsMetadata())
//    {
//      for(PartitionMetadata pm : tm.partitionsMetadata())
//      {
//        allPartitions.add(pm.partitionId());
//        System.out.println(pm.partitionId());
//      }
//    }

  }
}

