package com.test;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;

public class bb implements Runnable {

	public void run() {
		// TODO Auto-generated method stub
		ActiveMQConnectionFactory jmsConnectionFactory = new ActiveMQConnectionFactory("tcp://192.168.99.103:61616");
		try {
			Connection con = jmsConnectionFactory.createConnection();
			con.setClientID("Storm");
			con.start();
			Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Topic tt = session.createTopic("ForStorm");
			TopicSubscriber ts = session.createDurableSubscriber(tt, "Test_Durable_Subscriber");
			MessageListener listner = new MessageListener() {

				public void onMessage(Message message) {
					if (message instanceof ActiveMQBytesMessage) {
						ActiveMQBytesMessage textMessage = (ActiveMQBytesMessage) message;
						try {
							String messageText = new String(textMessage.getContent().data);
							System.out.println(messageText);
							message.acknowledge();
						} catch (JMSException e1) {
							// TODO Auto-generated catch block	
							e1.printStackTrace();
						}
					}
				}
			};
			ts.setMessageListener(listner);
	

		} catch (JMSException e) {
			e.printStackTrace();
		}

	}

}
