package com.prd.utilities;

import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class Email_sender {
	
	  
			public void mailsender(String Storename) {
		      
		        final String username = "nandankabra1@gmail.com";
		        final String password = "dznn wtxs rirx gljo";

		        Properties props = new Properties();
		        props.put("mail.smtp.auth", "true");
		        props.put("mail.smtp.starttls.enable", "true");
		        props.put("mail.smtp.host", "smtp.gmail.com"); // Replace with your SMTP server
		        props.put("mail.smtp.port", "587");
		        Session session = Session.getInstance(props, new Authenticator() {
		            @Override
		            protected PasswordAuthentication getPasswordAuthentication() {
		                return new PasswordAuthentication(username, password);
		            }
		        });

		        try {
		            
		            Message message = new MimeMessage(session);
		            message.setFrom(new InternetAddress(username));
		            InternetAddress[] toAddresses = {
		                    //new InternetAddress("vikas@techryde.com")
		            		new InternetAddress("nandank.kabra@techryde.com"),
		            		//new InternetAddress("nandankabra@gmail.com"),
		            		//new InternetAddress("gursehaj.singh@techryde.com"),
		            	    new InternetAddress("sandeep@techryde.com"),
		            	    new InternetAddress("krupal@techryde.com")
		            };
		            message.setRecipients(Message.RecipientType.TO, toAddresses );
		            message.setSubject("Techryde Alerts : Internal ");
		            message.setText("Hi,\n "+Storename);
		            Transport.send(message);
		            System.out.println("Email sent successfully.");
		        } catch (MessagingException e) {
		            e.printStackTrace();
		            System.err.println("Error sending email: " + e.getMessage());
		        }
		    }

}
