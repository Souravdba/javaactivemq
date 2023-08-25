package com.test;

import java.util.Map;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class JMSSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private MessageListener listner;
	private TopicSubscriber ts;

	public void nextTuple() {
		// TODO Auto-generated method stub
		MessageListener listner = new MessageListener() {

			public void onMessage(Message message) {
				if (message instanceof ActiveMQBytesMessage) {
					ActiveMQBytesMessage textMessage = (ActiveMQBytesMessage) message;
                    
					String messageText = new String(textMessage.getContent().data);
					// System.out.println(messageText);
					collector.emit(new Values("messageText"));
					// message.acknowledge();

				}
			}
		};
		try {
			ts.setMessageListener(listner);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void open(Map conf, TopologyContext tuple, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		ActiveMQConnectionFactory jmsConnectionFactory = new ActiveMQConnectionFactory("tcp://192.168.99.103:61616");
		Connection con;
		try {
			con = jmsConnectionFactory.createConnection();
			con.setClientID("Storm");
			con.start();
			Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
//			Topic tt = session.createTopic("ForStorm");
			Queue  tt = session.createQueue("ForStorm");
//			ts = session.createDurableSubscriber(tt, "Test_Durable_Subscriber");
			
			
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("Message"));

	}

}
