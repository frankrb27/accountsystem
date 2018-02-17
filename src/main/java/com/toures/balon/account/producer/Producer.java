package com.toures.balon.account.producer;

import java.util.Random;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
 
public class Producer {
 
    private static final Random RANDOM = new Random(System.currentTimeMillis());
 
    private static final String URL = "tcp://localhost:61616";
 
    private static final String USER = ActiveMQConnection.DEFAULT_USER;
 
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
 
    private static final String DESTINATION_QUEUE = "ACCOUNT.QUEUE";
 
    private static final boolean TRANSACTED_SESSION = true;
    
    private static final int MESSAGES_TO_SEND = 20;
 
    private String DEFAULT_MESSAGE = "1000001010203073CC                              FRANK JAIDER RODRIGUEZ BARRETO09840001000000";
    /**
     * 
     * @throws JMSException
     */
    public void sendMessages() throws JMSException {
 
        final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USER, PASSWORD, URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();
 
        final Session session = connection.createSession(TRANSACTED_SESSION, Session.AUTO_ACKNOWLEDGE);
        final Destination destination = session.createQueue(DESTINATION_QUEUE);
 
        final MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
 
        sendMessages(session, producer);
        session.commit();
 
        session.close();
        connection.close();
 
        System.out.println("Mensajes enviados correctamente");
    }
 
    /**
     * 
     * @param session
     * @param producer
     * @throws JMSException
     */
    private void sendMessages(Session session, MessageProducer producer) throws JMSException {
        final Producer messageSender = new Producer();
        for (int i = 1; i <= MESSAGES_TO_SEND; i++) {
            messageSender.sendMessage(DEFAULT_MESSAGE, session, producer);
        }
    }
 
    /**
     * 
     * @param message
     * @param session
     * @param producer
     * @throws JMSException
     */
    private void sendMessage(String message, Session session, MessageProducer producer) throws JMSException {
        final TextMessage textMessage = session.createTextMessage(message);
        producer.send(textMessage);
    }
 
    /**
     * 
     * @param args
     * @throws JMSException
     */
    public static void main(String[] args) throws JMSException {
        final Producer messageSender = new Producer();
        messageSender.sendMessages();
    }
 
}