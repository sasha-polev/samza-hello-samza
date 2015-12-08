package org.onepoint.samza.system;


import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
import java.util.*;

public class JMSConsumer  implements SystemConsumer {
    private String systemName;
    private String host;
    private Integer port;
    private String topic;
    private final List<String> channels;
    MessageConsumer consumer;

    @Override
    public void register(SystemStreamPartition systemStreamPartition, String startingOffset) {
    }

    @Override
    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
        Message msg = null;
        Map<SystemStreamPartition, List<IncomingMessageEnvelope>> m = new HashMap<SystemStreamPartition, List<IncomingMessageEnvelope>>();
        try {
            //msg = consumer.receiveNoWait();
            msg = consumer.receive(10);
            while(msg != null) {
                System.out.println("Got message...");
                if (msg instanceof TextMessage) {
                    System.out.println("Message text is : " + ((TextMessage) msg).getText());
                    SystemStreamPartition next = systemStreamPartitions.iterator().next();
                    List<IncomingMessageEnvelope> l = new ArrayList<IncomingMessageEnvelope>();
                    l.add(new IncomingMessageEnvelope(next, null, null, ((TextMessage) msg).getText()));
                    m.put(next, l);
                }
                msg = consumer.receiveNoWait();
            }
        } catch (JMSException e) {
            e.printStackTrace();
            return null;
        }
        return m ;

    }

    public JMSConsumer(String systemName, String host, Integer port, String jmsTopic, MetricsRegistry registry) {
        this.systemName = systemName;
        this.host = host;
        this.port = port;
        this.topic = jmsTopic;
        this.channels = new ArrayList<String>();
    }
    @Override
    public void start() {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("consumer_login", "consumer_password", "tcp://" + host + ":" + port.toString());



        try {
            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();
            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)

            Topic destination = session.createTopic(topic);
            consumer = session.createConsumer(destination);
            System.out.println("JMS Receiver waiting for messages...");

        } catch (JMSException e) {
            e.printStackTrace();
        }


        //connection.setExceptionListener(this);


    }

    @Override
    public void stop() {

    }
}
