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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;



public class KettleJobTask implements StreamTask, InitableTask, WindowableTask {

  private Set<String> messages = new HashSet<String>();
  RowMetaInterface rowMeta = null;
  Trans trans = null;

  public void init(Config config, TaskContext context) {
    String plugins_location = config.get("task.plugins");
    String transformation_location = config.get("task.transformation");
    String transformation_authtoken= config.get("task.authToken");




    try {
      if (!KettleEnvironment.isInitialized()) {
        System.setProperty("KETTLE_PLUGIN_BASE_FOLDERS", plugins_location);
        KettleEnvironment.init();
      }
      TransMeta metadata = new TransMeta(transformation_location);


      trans = new Trans(metadata);
      trans.setParameterValue("authToken", transformation_authtoken);
      trans.activateParameters();
      trans.prepareExecution(null);
    } catch (KettleValueException e) {
      //			e.printStackTrace();
    } catch (KettleException e) {
      e.printStackTrace();
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    messages.add(envelope.getMessage().toString());
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {

    try {
      trans.prepareExecution(null);
      if(rowMeta == null){
        StepMeta si = trans.getStepInterface("Injector", 0).getStepMeta();
        TransMeta parentTransMeta = si.getParentTransMeta();
        rowMeta = parentTransMeta.getStepFields(si);
      }
      RowProducer rp = trans.addRowProducer("Injector", 0);
      trans.startThreads();
      for(String message: messages)
      {
        System.out.println(message);
        String[] myStrings = message.split("\\|");
        rp.putRow(rowMeta, myStrings);
      }

      rp.finished();
      trans.waitUntilFinished();
      Result result = trans.getResult();
      collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "job-results"), result.toString()));
    } catch (KettleException e) {
      e.printStackTrace();
    }

    messages = new HashSet<String>();
  }
}
