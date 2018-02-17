package com.toures.balon.account.consumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Consumer {
	
	private static String RESOURCE_BUNDLE_APP = "com.toures.balon.account.application";

	private static ResourceBundle bundleApp = ResourceBundle.getBundle(RESOURCE_BUNDLE_APP);
	
	private static final String URL = bundleApp.getString("activemq.url");
	 
    private static final String USER = ActiveMQConnection.DEFAULT_USER;
 
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
 
    private static final String DESTINATION_QUEUE = "ACCOUNT.QUEUE";
 
    private static final boolean TRANSACTED_SESSION = false;
 
    private static final int TIMEOUT = 1000;
    
    private static final String PATH = bundleApp.getString("file.read");
    
    private static final String FILE = bundleApp.getString("file.name");
    
    private static final String EXTENSION = bundleApp.getString("file.extension");
 
    private final Map<String, Integer> consumedMessageTypes;
 
    private int totalConsumedMessages = 0;
 
    /**
     * 
     */
    public Consumer() {
        this.consumedMessageTypes = new HashMap<String, Integer>();
    }
 
    /**
     * 
     * @throws JMSException
     * @throws Exception
     */
    public void processMessages() throws JMSException, Exception{
 
        final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USER, PASSWORD, URL);
        final Connection connection = (Connection) connectionFactory.createConnection();
 
        connection.start();
 
        final Session session = connection.createSession(TRANSACTED_SESSION, Session.AUTO_ACKNOWLEDGE);
        final Destination destination = session.createQueue(DESTINATION_QUEUE);
        final MessageConsumer consumer = session.createConsumer(destination);
 
        processAllMessagesInQueue(consumer);
 
        consumer.close();
        session.close();
        connection.close();
 
        showProcessedResults();
    }
 
    /**
     * 
     * @param consumer
     * @throws JMSException
     * @throws Exception
     */
    private void processAllMessagesInQueue(MessageConsumer consumer) throws JMSException, Exception {
        Message message;
        while ((message = consumer.receive(TIMEOUT)) != null) {
            proccessMessage(message);
        }
    }
 
    /**
     * 
     * @param message
     * @throws JMSException
     * @throws Exception
     */
    private void proccessMessage(Message message) throws JMSException, Exception {
        if (message instanceof TextMessage) {        	
        	final TextMessage textMessage = (TextMessage) message;
            final String text = textMessage.getText();
            String fileName = getFileName();
        	//File archivo = new File(PATH.concat(fileName));
        	BufferedWriter bw = new BufferedWriter(new FileWriter(PATH.concat(fileName), true));
            bw.append(text);
            bw.append("\n");
            bw.close();            
            incrementMessageType(text);
            totalConsumedMessages++;
        }
    }
 
    /**
     * 
     * @param message
     */
    private void incrementMessageType(String message) {
        if (consumedMessageTypes.get(message) == null) {
            consumedMessageTypes.put(message, 1);
        } else {
            final int numberOfTypeMessages = consumedMessageTypes.get(message);
            consumedMessageTypes.put(message, numberOfTypeMessages + 1);
        }
    }
 
    /**
     * 
     */
    private void showProcessedResults() {
        System.out.println("Procesados un total de " + totalConsumedMessages + " mensajes");
        for (String messageType : consumedMessageTypes.keySet()) {
            final int numberOfTypeMessages = consumedMessageTypes.get(messageType);
            System.out.println("Tipo " + messageType + " Procesados " + numberOfTypeMessages + " (" +
                    (numberOfTypeMessages * 100 / totalConsumedMessages) + "%)");
        }
    }
 
    /**
     * 
     * @return
     */
    private String getFileName() {
    	String fileName = FILE;
    	Calendar c = Calendar.getInstance();
    	fileName = fileName.concat(""+c.get(Calendar.YEAR)).concat(""+c.get((Calendar.MONTH+1))).concat(""+c.get(Calendar.DAY_OF_MONTH)).concat(EXTENSION);
    	return fileName;
    }
    
    /**
     * 
     * @param args
     * @throws JMSException
     * @throws Exception
     */
    public static void main(String[] args) throws JMSException, Exception {
        final Consumer consumer = new Consumer();
        consumer.processMessages();
    }
}
