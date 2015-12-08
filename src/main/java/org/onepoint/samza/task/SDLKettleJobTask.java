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
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.*;


public class SDLKettleJobTask implements StreamTask, InitableTask, WindowableTask {

  private Map<Map<String,Object>, String> messages = new HashMap<Map<String,Object>,String>();
  RowMetaInterface rowMeta = null;
  Set<String> uniqueCampaigns = new HashSet<String>();
  Trans trans = null;
  private KeyValueStore<String, Integer> store;
  String transformation_authtoken = null;

  public void init(Config config, TaskContext context) {
    String plugins_location = config.get("task.plugins");
    String transformation_location = config.get("task.transformation");
    transformation_authtoken= config.get("task.authToken");
    store = (KeyValueStore<String, Integer>) context.getStore("pdi-store");
    //store.put("testabc",1);
    //System.out.println("should be 1: " + store.get("testabc"));

    try {
      if (!KettleEnvironment.isInitialized()) {
        System.setProperty("KETTLE_PLUGIN_BASE_FOLDERS", plugins_location);
        KettleEnvironment.init();
      }


      TransMeta metadata = new TransMeta(transformation_location);


      trans = new Trans(metadata);

      //trans.prepareExecution(null);
    } catch (KettleValueException e) {
      e.printStackTrace();
    } catch (KettleException e) {
      e.printStackTrace();
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    Object message = envelope.getMessage();
    String key = envelope.getKey().toString();
    //System.out.println("got message of type: " + message.getClass() + "with key: " + key);
    if(message instanceof Map && key.contains("^")) {
      messages.put((Map<String, Object>) message, key);
      //System.out.println("enqueued message for campaign: " + key);
      if(!uniqueCampaigns.contains(key))
        uniqueCampaigns.add(key);
    }
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {

    try {
      System.out.println("Number of unique campaigns registered: " + uniqueCampaigns.size());
      //group per campaing
      for(String campaign : uniqueCampaigns) {


        if (messages.containsValue(campaign)) {
          //System.out.println("Got messages for campaign: " + campaign + "Total messages  across all campaigns is: " + messages.size());

          trans.setParameterValue("authToken", transformation_authtoken);
          trans.activateParameters();
          trans.prepareExecution(null);
          if (rowMeta == null) {
            StepMeta si = trans.getStepInterface("Injector", 0).getStepMeta();
            TransMeta parentTransMeta = si.getParentTransMeta();
            rowMeta = parentTransMeta.getStepFields(si);
          }
          //campaignID^creativeInstanceId^event1^eventSourceId"
          String[] campaignDecoded = campaign.split("\\^");
          RowProducer rp = trans.addRowProducer("Injector", 0);
          trans.startThreads();
          for (Map.Entry<Map<String, Object>, String> message : messages.entrySet()) {
            if (message.getValue().equals(campaign)) {
              Map<String, Object> eventMap = message.getKey();
              ArrayList<Object> value = new ArrayList<Object>();
              String storeKey = campaign;
              for (String name : rowMeta.getFieldNames()) {
                if (eventMap.containsKey(name) && eventMap.get(name) != null) {
                  value.add(eventMap.get(name));
                  if (name.equals("Key"))
                    storeKey += "|" + eventMap.get(name);
                } else if (name.equals("campaignId")) {
                  value.add(campaignDecoded[0]);
                } else if (name.equals("CreativeInstanceKeyColumn")) {
                  value.add(campaignDecoded[1]);
                } else if (name.equals("EventColumn")) {
                  value.add(campaignDecoded[2]);
                } else if (name.equals("eventSource")) {
                  value.add(campaignDecoded[3]);
                }
              }
              //Check if event was already sent today
              //System.out.println("storeKey: "+ storeKey);
              if (store.get(storeKey) == null) {
                Object[] values = value.toArray(new Object[value.size()]);
                rp.putRow(rowMeta, values);
                store.put(storeKey, 1);
              }
            }
          }
          rp.finished();
          trans.waitUntilFinished();
          Result result = trans.getResult();
          collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "job-results"), result.toString()));
        }
      }

    } catch (KettleException e) {
      e.printStackTrace();
    }

    //reset data (safe as single thread
    messages = new HashMap<Map<String,Object>,String>();
    uniqueCampaigns = new HashSet<String>();
  }
}
