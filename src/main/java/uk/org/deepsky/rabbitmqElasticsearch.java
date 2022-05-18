package uk.org.deepsky;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class rabbitmqElasticsearch {
    static Properties props = new Properties();
    static boolean debug = false;
    public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException {

        try (InputStream configStream = new FileInputStream("config.properties")) {
            props.load(configStream);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        if (props.getProperty("debug").toLowerCase().equals("true")) {
            System.out.println("Debug Mode: " + props.getProperty("debug"));
            debug = true;
        };
        ConnectionFactory connfac = new ConnectionFactory();
        connfac.setHost(props.getProperty("amqp.host"));
        connfac.setPort(Integer.parseInt(props.getProperty("amqp.port")));
        connfac.setUsername(props.getProperty("amqp.user"));
        connfac.setPassword(props.getProperty("amqp.pass"));
        connfac.setVirtualHost(props.getProperty("amqp.vhost"));
        connfac.useSslProtocol();
        try {
            Connection amqpCon = connfac.newConnection();
            Channel channel = amqpCon.createChannel();
            boolean autoAck = false;
            channel.basicConsume(props.getProperty("amqp.queue"), autoAck, "myConsumerTag",
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                Envelope envelope,
                                AMQP.BasicProperties properties,
                                byte[] body)
                                throws IOException {
                            // String routingKey = envelope.getRoutingKey();
                            // String contentType = properties.getContentType();
                            long deliveryTag = envelope.getDeliveryTag();

                            try {
                                int responseCode = sendToElasticsearch(new String(body, "UTF-8"));
                                if (responseCode == 200 || responseCode == 201) {
                                    debug("\nSending AMQP ACK ");
                                    channel.basicAck(deliveryTag, true);
                                } else {
                                    debug("\nSending AMQP NACK ");
                                    channel.basicAck(deliveryTag, false);

                                }
                                System.out.println("Elasticsearch Index:"+ props.getProperty("es.index") + " Response:" + responseCode + " ");

                            } catch (Exception e) {
                                channel.basicAck(deliveryTag, false);
                                debug(e.getMessage());
                                e.printStackTrace();
                            }
                        }
                    });

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static int sendToElasticsearch(String message) throws Exception {

        URL url = new URL("http://" + props.getProperty("es.host") + ":" + props.getProperty("es.port") + "/"
                + props.getProperty("es.index") + "/_doc/");
        HttpURLConnection myURLConnection = (HttpURLConnection) url.openConnection();

        String userCredentials = props.getProperty("es.user") + ":" + props.getProperty("es.password");
        String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

        myURLConnection.setRequestProperty("Authorization", basicAuth);

        myURLConnection.setRequestMethod("POST");
        myURLConnection.setDoOutput(true);
        myURLConnection.setRequestProperty("Accept", "application/json");
        myURLConnection.setRequestProperty("Content-Type", "application/json");

        byte[] out = message.getBytes(StandardCharsets.UTF_8);

        OutputStream stream = myURLConnection.getOutputStream();
        stream.write(out);
        myURLConnection.disconnect();
        debug("JSON:"+ new String(out));
        return myURLConnection.getResponseCode();

    }


    private static void debug(String message){
        if (debug) System.out.print(message);

    }
}
