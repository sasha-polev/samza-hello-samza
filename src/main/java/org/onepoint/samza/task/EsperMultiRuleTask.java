package org.onepoint.samza.task;

import com.espertech.esper.client.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.util.*;


public class EsperMultiRuleTask implements StreamTask,InitableTask, WindowableTask {
    public static final String COMMAND_EVENT_TYPE = "command-event-type-";
    public static final String COMMAND_ESPER_EXPRESSION = "command-esper-expression-";
    private SystemStream OUTPUT_STREAM;
    private static final SystemStream LOG_STREAM = new SystemStream("kafka","esper-log-output");
    private EPServiceProvider esperProvider;
    private Map<String, EPStatement> statementMap = new HashMap<String, EPStatement>();
    private static final Log log = LogFactory.getLog(EsperMultiRuleTask.class);
    private Map<String, String> subscriberNames = new HashMap<String,String>();
    private HashSet<String> eventNames = new HashSet<String>();
    private Map<String, Class> classMap = new HashMap<String, Class>();
    private HashSet<Map<String,Object>> seenEvents = new HashSet<Map<String,Object>>();
    //private String keyAttribute = "Key";
    //private Long nextMidtime =



    @Override
    public void init(Config config, TaskContext tc) throws Exception {
        esperProvider = EPServiceProviderManager.getDefaultProvider();
        OUTPUT_STREAM = new SystemStream("kafka", config.get("task.output-stream"));
        classMap.put("string", String.class);
        classMap.put("int", int.class);
        classMap.put("long", Long.class);
        classMap.put("date", Date.class);
        classMap.put("float", Float.class);
        //Bootstrap event types known in advance
        registerKnownEvents();
    }

    private void registerKnownEvents() {
        //UsageEvent
        String[] propertyNames = {"Key","volume"};
        Object[] propertyTypes = {String.class,int.class};
        esperProvider.getEPAdministrator().getConfiguration().
                addEventType("UsageEvent", propertyNames, propertyTypes);
        eventNames.add("UsageEvent");
        //Call Event
        String[] propertyNames1 = {"Key","Start","End","Duration","Call_Type","Confidence", "StartLat","StartLong","EndLat","EndLong"};
        Object[] propertyTypes1 = {String.class,Long.class,Long.class,Long.class,String.class,String.class,Float.class,Float.class,Float.class,Float.class};
        esperProvider.getEPAdministrator().getConfiguration().
                addEventType("CallEvent", propertyNames1, propertyTypes1);
        eventNames.add("CallEvent");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(IncomingMessageEnvelope ime, MessageCollector mc, TaskCoordinator tc) throws Exception {

        Object rawMessage = ime.getMessage();
        Object key = ime.getKey();

        //System.out.println("Incoming message: " + ime);
        //System.out.println("Key: " + key);
        //System.out.println("Raw message: " + rawMessage);
        if(key != null && key  instanceof String &&  ((String)key).startsWith(COMMAND_EVENT_TYPE) && rawMessage instanceof Map)
        {

/*        Types should be deserialised in a format:
          String[] propertyNames = {"carId", "direction"};   // order is important
          Object[] propertyTypes = {String.class, int.class}
          For speed we will use static map of allowed class names and classes
          For now just String or int
*/
            String key1 = ((String)key).replace(COMMAND_EVENT_TYPE, "");
            if(!eventNames.contains(key1)) {
                mc.send(new OutgoingMessageEnvelope(LOG_STREAM, "got new type: " + key));
                Map<String, String> mapMessage = (Map<String, String>) rawMessage;
                //System.out.println("Map: " + mapMessage);
                String[] propertyNames = mapMessage.keySet().toArray(new String[mapMessage.size()]);
                Object[] propertyTypes = new Class[mapMessage.size()];
                int i = 0;
                for (String entry : mapMessage.values()) {
                    propertyTypes[i] = classMap.get(entry.toLowerCase());
                    i++;
                }

                esperProvider.getEPAdministrator().getConfiguration().
                        addEventType(key1, propertyNames, propertyTypes);
                eventNames.add(key1);
            }

        }
        else if(key != null && key  instanceof String && ((String)key).startsWith(COMMAND_ESPER_EXPRESSION))
        {

            if(!subscriberNames.keySet().contains(key) ) {
                String statement = rawMessage.toString();
                if(rawMessage instanceof Map) {
                    Map<String, String> mapMessage = (Map<String, String>) rawMessage;
                    statement = mapMessage.get("statement");
                }
                mc.send(new OutgoingMessageEnvelope(LOG_STREAM, "got new statement: " + statement));
                EPStatement stmt = null;
                if (!statementMap.containsKey(statement)) {
                    stmt = esperProvider.getEPAdministrator().createEPL(statement);
                    statementMap.put(statement, stmt);
                } else
                    stmt = statementMap.get(statement);

                //stmt.addListener(new EventListener(mc, ((String)key).replace(COMMAND_ESPER_EXPRESSION, "")));
                subscriberNames.put(((String) key), statement);
                System.out.println("Total statements registered : " + statementMap.size());

            }
        }
        else if(rawMessage instanceof Map)
        {
            String eventType = ((Map<String, Object>) rawMessage) .get("eventType").toString();
            ArrayList<Object> value = (ArrayList<Object>) ((Map<String, Object>) rawMessage).get("value");
            Object [] values = value.toArray(new Object [value.size()]);
            if(eventNames.contains(eventType)) {
                //mc.send(new OutgoingMessageEnvelope(LOG_STREAM, "sending values to runtime: " + values));
                esperProvider.getEPRuntime().sendEvent(values, eventType);
            }
        }
        else
            System.out.println("Unknown message with key: " + key.toString());
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        collector.send(new OutgoingMessageEnvelope(LOG_STREAM, "current size of seen events set: " + seenEvents.size()));
        for(Map.Entry<String, EPStatement> entry: statementMap.entrySet())
        //for(EPStatement st: statementMap.values())
        {
            //get campaing IDs for statement
            EPStatement st = entry.getValue();
            String statement = entry.getKey();


            Iterator<EventBean> iter = st.iterator();
            for (;iter.hasNext();) {
                EventBean event = iter.next();
//                collector.send(new OutgoingMessageEnvelope(LOG_STREAM, "got output event: " + event.getEventType().toString()));

//                    collector.send(new OutgoingMessageEnvelope(LOG_STREAM, "got unseen event: " + event));

                    Map<String,Object> output = new HashMap<String, Object>();
                    String[] propertyNames = event.getEventType().getPropertyNames();
                    for(String propName: propertyNames)
                        output.put(propName, event.get(propName));
                //String hashKeyString = Integer.toString(output.hashCode()) + output.get(keyAttribute).toString();
                if(!seenEvents.contains(output))
                {
                    seenEvents.add(output);
                    for(Map.Entry<String, String> subscriber: subscriberNames.entrySet())
                        if(subscriber.getValue().equals(statement))
                            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, null, null, subscriber.getKey().replace(COMMAND_ESPER_EXPRESSION,""), output));
                }
            }

        }
    }

//    private class EventListener implements UpdateListener
//    {
//        private final MessageCollector collector;
//        private String extraArgs;
//
//        public EventListener(MessageCollector collector, String extraArgs)
//        {
//            this.collector = collector;
//            this.extraArgs = extraArgs;
//        }
//
//        @Override
//        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
//            if(newEvents != null)
//            {
//                for(EventBean event : newEvents)
//                {
//                    collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, null, null, extraArgs,  event));
//                }
//            }
//        }
//
//    }
}
