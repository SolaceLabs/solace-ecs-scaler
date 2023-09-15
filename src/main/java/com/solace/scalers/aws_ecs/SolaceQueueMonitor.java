package com.solace.scalers.aws_ecs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.solace.scalers.aws_ecs.model.semp_v2.SempQueueResponse;

/**
 * Class SolaceQueueMonitor provides a facility to monitor queues for specific values
 * using SEMPv2. This class is designed to extract data to monitor Solace PS+ queues
 * TODO - Refactor for efficiency at scale; e.g. use SEMP over message bus?
 * TODO - Add mTLS authentication to Solace PS+ brokers
 * TODO - Add configurable ability to skip SSL certificate-host verification
 * TODO - Define and add logic to better handle http failures
 */
public class SolaceQueueMonitor {
    
    private static final Logger logger = LogManager.getLogger(SolaceQueueMonitor.class);

    private static final String SEMP_URL_QUERY_STRING = "?select=msgs.count,msgVpnName,queueName,msgSpoolUsage,averageRxMsgRate,averageTxMsgRate",
                                SEMP_URL_FORMAT       = "%s/SEMP/v2/monitor/msgVpns/%s/queues/%s%s";

    private URL url = null;

    private String username = "";

    private String password = "";

    private String queueName = "";

    /**
     * Get queueName associated with this object
     * @return
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Set queueName associated with this object
     * @param queueName
     */
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    /**
     * Returns URL object for this instance of the class. Returns NULL if the URL has not
     * been initialized
     * @return
     */
    public URL getUrl() {
        return this.url;
    }

    /**
     * Set User Name for HTTP basic auth
     * @param userName
     */
    public void setUsername(String userName) {
        this.username = userName;
    }

    /**
     * Set Password for HTTP basic auth
     * @param password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    public static String formatUrl( 
                String brokerSempUrl,
                String msgVpnName, 
                String queueName ) {
        return
            String.format( SEMP_URL_FORMAT, brokerSempUrl, msgVpnName, queueName, SEMP_URL_QUERY_STRING );
    }
    
    /**
     * Constructor can be used to pass a URL as string to the object. 
     * @param url
     * @throws MalformedURLException
     */
    public SolaceQueueMonitor( String url, String queueName ) throws MalformedURLException {
        this.url = new URL( url );
        this.queueName = queueName;
    }

    /**
     * Executes a call to SEMP to retrieve a monitoring record for the queue found
     * at the configured endpoint. Returns an Object of type `MsgVpnQueueResponse`
     * defined by the SEMPv2 OpenAPI spec
     * @return
     * @throws IOException
     * @throws JsonSyntaxException
     */
    public synchronized SempQueueResponse getSempMonitorForQueue() throws IOException, JsonSyntaxException {
        
        HttpURLConnection connection = ( HttpURLConnection )url.openConnection();
        
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "application/json");

        String authHeader = formatHttpBasicAuthHeader( this.username, this.password );
        if ( authHeader != null ) {
            connection.setRequestProperty("Authorization", authHeader);
        }
        connection.setConnectTimeout( 2500 );       // 2.5 seconds

        // Connect and GET the data
        connection.connect();

        int responseCode = connection.getResponseCode();
        if (responseCode < 200 || responseCode > 204 ) {
            logger.error( "QueueName={} -- Call to SEMP responseCode = {}", 
                            queueName, responseCode );
            logger.error( "QueueName={} -- SEMP Response Message: {}", 
                            queueName, connection.getResponseMessage() );
            connection.disconnect();
            throw new IOException( 
                        "SEMP: responseCode=" + responseCode + 
                        " message=" + connection.getResponseMessage() );
        }

        // Get data from the input stream
        BufferedReader in = new BufferedReader(
            new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuffer content = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();

        // Close the connection
        connection.disconnect();

        // Parse the result and return as object
        // TODO - Define customer object instead of using SEMPv2 generated classes?
        Gson gson = new Gson();
        return gson.fromJson(content.toString(), SempQueueResponse.class);
    }

    /**
     * Formats HTTP Authorization header from username and password
     * @param userName
     * @param password
     * @return Base64 encoded Http Basic Auth Header
     */
    public static String formatHttpBasicAuthHeader( String userName, String password) {
        if ( userName == null || password == null ) {
            return null;
        }
        String userNamePassword = userName + ":" + password;
        String authHeader = "Basic " + Base64.getEncoder().encodeToString( userNamePassword.getBytes() );
        return authHeader;
    }
}