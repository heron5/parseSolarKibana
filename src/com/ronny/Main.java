package com.ronny;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.elasticsearch.client.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;  // Import the IOException class to handle errors
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.lang.*;



import org.apache.http.HttpHost;

public class Main {
    public void updateDb(long source, long status, long totalKWh, long currentW, long todayKWh,
                         double currentDcA1, double currentDcV1, double currentDcW1,
                         double currentDcA2, double currentDcV2, double currentDcW2,
                         String timeOffset, String host, String port, int loggLevel) {

        SensorProperties sensor = new SensorProperties();

        if (!sensor.getSensorProperties(Long.toString(source)))
            return;

        lookupSpotPrice spotPrice;
        Double price = 0.00;
        Double averagePrice = 0.00;
        try {
            spotPrice = new lookupSpotPrice();
            spotPrice.lookupSpot();
            price = spotPrice.price;
            averagePrice = spotPrice.averagePrice;
        }catch (Exception e){
            System.out.println(e);
        }
        long sourceId = sensor.sourceId;
        String description = sensor.shortDescription;
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss" + timeOffset.trim());
        LocalDateTime now = LocalDateTime.now();


        JSONObject jsonBody = new JSONObject();
        JSONObject jsonTags = new JSONObject();
        JSONObject jsonFields = new JSONObject();

        jsonTags.put("source", sourceId);
        jsonTags.put("source_desc", description);
        jsonFields.put("status", status);
        jsonFields.put("total_kWh", totalKWh);
        jsonFields.put("current_W", currentW);
        jsonFields.put("today_kWh", todayKWh);
        jsonFields.put("current_spot", price);
        jsonFields.put("average_spot", averagePrice);
        jsonFields.put("current_DC_A_1", currentDcA1);
        jsonFields.put("current_DC_V_1", currentDcV1);
        jsonFields.put("current_DC_W_1", currentDcW1);
        jsonFields.put("current_DC_A_2", currentDcA2);
        jsonFields.put("current_DC_V_2", currentDcV2);
        jsonFields.put("current_DC_W_2", currentDcW2);

        jsonBody.put("measurement", "SolarLogg");
        jsonBody.put("date", dtf.format(now));
        jsonBody.put("tags", jsonTags);
        jsonBody.put("fields", jsonFields);

        if (loggLevel > 1)
            System.out.println(jsonBody);

        try {
            if (totalKWh < 4_000_000_000L) {
                FileWriter jsonFile = new FileWriter("/tmp/energy_produced.json", false);
                jsonFile.write(jsonBody + "\n");
                jsonFile.close();
            }
        } catch (IOException e) {
            System.err.print("FileWriter went wrong");
            e.printStackTrace();
        }


        HttpHost esHost = new HttpHost(host, Integer.parseInt(port));
        RestClient restClient = RestClient.builder(esHost).build();

        Request request = new Request(
                "POST",
                "/solar_index/solarlog");

        request.setJsonEntity(jsonBody.toString());

        Response response = null;
        try {
            response = restClient.performRequest(request);
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (loggLevel > 0)
            System.out.println(response);
    }


    public void run() {
        System.out.println("TopicSubscriber initializing...");
        System.out.println("Get properties from file...");

        String host = "";
        String username = "";
        String password = "";
        String topic = "";
        String timeOffset = "";
        String kibanaHost = "";
        String kibanaPort = "";
        int loggLevel = 0;


        try (InputStream input = new FileInputStream("parseSolarKibana.properties")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            host = prop.getProperty("mqttHost");
            username = prop.getProperty("mqttUsername");
            password = prop.getProperty("mqttPassword");
            topic = prop.getProperty("mqttTopic");
            timeOffset = prop.getProperty("timeOffset");
            kibanaHost = prop.getProperty("kibanaHost");
            kibanaPort = prop.getProperty("kibanaPort");
            loggLevel = Integer.parseInt( prop.getProperty("loggLevel"));
            System.out.println("Logglevel: "+ loggLevel);

        } catch (IOException ex) {
            ex.printStackTrace();
        }



        try {
            // Create an Mqtt client
            Random rand = new Random();
            MqttClient mqttClient = new MqttClient(host, "parseSolar" + rand.toString());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(username);
            connOpts.setPassword(password.toCharArray());
            connOpts.setAutomaticReconnect(true);

            // Connect the client
            System.out.println("Connecting to Kjuladata messaging at " + host);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            // Topic filter the client will subscribe to
            final String subTopic = topic;

            // Callback - Anonymous inner-class for receiving messages
            String finalTimeOffset = timeOffset;
            String finalKibanaHost = kibanaHost;
            String finalKibanaPort = kibanaPort;
            int finalLoggLevel = loggLevel;
            mqttClient.setCallback(new MqttCallback() {
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // Called when a message arrives from the server that
                    // matches any subscription made by the client

                    JSONParser parser = new JSONParser();
                    String payLoad = new String(message.getPayload());

                    if (finalLoggLevel > 1)
                        System.out.println(payLoad);
                    JSONObject json = (JSONObject) parser.parse(payLoad);
                    JSONArray solarlogg = (JSONArray) json.get("solarlogg");

                    /*
                    updateDB( i['status'], i['source'], i['total_kWh'], i['current_W'], i['today_kWh'],
                            i['current_DC_A_1'], i['current_DC_V_1'], i['current_DC_W_1'],
                            i['current_DC_A_2'], i['current_DC_V_2'], i['current_DC_W_2'])

                     */
                    try {
                        for (Object logg : solarlogg) {
                            JSONObject loggbysource = (JSONObject) logg;
                            long source = (long) loggbysource.get("source");
                            long status = (long) loggbysource.get("status");
                            long totalKWh = (long) loggbysource.get("total_kWh");
                            long currentW = (long) loggbysource.get("current_W");
                            long todayKWh = (long) loggbysource.get("today_kWh");
                            double currentDcA1 = (double) loggbysource.get("current_DC_A_1");
                            double currentDcV1 = (double) loggbysource.get("current_DC_V_1");
                            long currentDcW1 = (long) loggbysource.get("current_DC_W_1");
                            double currentDcA2 = (double) loggbysource.get("current_DC_A_2");
                            double currentDcV2 = (double) loggbysource.get("current_DC_V_2");
                            long currentDcW2 = (long) loggbysource.get("current_DC_W_2");

                            updateDb(source, status, totalKWh, currentW, todayKWh,
                                    currentDcA1, currentDcV1, currentDcW1,
                                    currentDcA2, currentDcV2, currentDcW2,
                                    finalTimeOffset, finalKibanaHost, finalKibanaPort, finalLoggLevel);
                        }
                    } catch (Exception pe) {
                        //  System.out.println("position: " + pe.getPosition());
                        System.out.println(pe);
                    }
                }

                public void connectionLost(Throwable cause) {
                    System.out.println("Connection to KjulaData messaging lost!" + cause);
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }

            });

            // Subscribe client to the topic filter and a QoS level of 0
            System.out.println("Subscribing client to topic: " + subTopic);
            mqttClient.subscribe(subTopic, 0);
            System.out.println("Subscribed");

        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Main().run();

    }
}
