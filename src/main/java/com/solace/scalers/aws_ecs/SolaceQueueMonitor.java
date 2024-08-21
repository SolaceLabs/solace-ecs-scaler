package com.solace.scalers.aws_ecs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import com.solace.scalers.aws_ecs.model.ScalerConfig;
import com.solace.scalers.aws_ecs.model.semp_v2.SempMessageVpnStateResponse;
import com.solace.scalers.aws_ecs.http.URLConnectionFactory;
import lombok.extern.log4j.Log4j2;
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
@Log4j2
public class SolaceQueueMonitor {
    private static final String SEMP_URL_QUERY_STRING = "?select=msgs.count,msgVpnName,queueName,msgSpoolUsage,averageRxMsgRate,averageTxMsgRate",
                                SEMP_URL_FORMAT       = "%s/SEMP/v2/monitor/msgVpns/%s/queues/%s%s",
                                SEMP_VPN_STATE_FORMAT = "%s/SEMP/v2/monitor/msgVpns/%s?select=state",
                                SEMP_VPN_STATE_UP = "up";
    public static final String ACTIVE_SEMP_CONFIG = "active",
                                STANDBY_SEMP_CONFIG = "standby";

    private Map<String,ScalerConfig.SempConfig> sempConfigMap;

    private String messageVpnName = "";

    private String queueName = "";

    private int numFailedRequestsInARow = 0;

    private final URLConnectionFactory connectionFactory;

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


    public Map<String, ScalerConfig.SempConfig> getSempConfigMap() {
        return sempConfigMap;
    }

    public void setSempConfigMap(Map<String, ScalerConfig.SempConfig> sempConfigMap) {
        this.sempConfigMap = sempConfigMap;
    }

    public String getMessageVpnName() {
        return messageVpnName;
    }

    public void setMessageVpnName(String messageVpnName) {
        this.messageVpnName = messageVpnName;
    }


    public static String formatQueueMonitorUrl(
                String brokerSempUrl,
                String msgVpnName, 
                String queueName) {
        return
            String.format( SEMP_URL_FORMAT, brokerSempUrl, msgVpnName, queueName, SEMP_URL_QUERY_STRING );
    }

    public static String formatVpnStateUrl(String brokerSempUrl,
                                           String msgVpnName) {
        return String.format(SEMP_VPN_STATE_FORMAT, brokerSempUrl, msgVpnName);
    }
    
    /**
     * Constructor can be used to pass a URL as string to the object. 
     * @param sempConfigMap
     * @throws MalformedURLException
     */
    public SolaceQueueMonitor(Map<String, ScalerConfig.SempConfig> sempConfigMap, String messageVpnName, String queueName, URLConnectionFactory connectionFactory ) throws MalformedURLException {
        this.sempConfigMap = sempConfigMap;
        this.messageVpnName = messageVpnName;
        this.queueName = queueName;
        this.connectionFactory = connectionFactory;
    }

    /**
     * Determines the active message vpn for the given broker configuration.
     * Executes a call to SEMP for the active vpn to retrieve a monitoring record for the queue found
     * at the configured endpoint. Returns an Object of type `MsgVpnQueueResponse`
     * defined by the SEMPv2 OpenAPI spec
     * @return
     * @throws IOException
     * @throws JsonSyntaxException
     */
    public synchronized SempQueueResponse getSempMonitorForQueue() throws IOException, JsonSyntaxException, URISyntaxException {
        updateActiveVpnForForQueueMonitor();

        Optional<String> optionalQueueMonitorResponse = getSempResponse(formatQueueMonitorUrl(sempConfigMap.get(ACTIVE_SEMP_CONFIG).getBrokerSempUrl(), messageVpnName, queueName), sempConfigMap.get(ACTIVE_SEMP_CONFIG).getUsername(), sempConfigMap.get(ACTIVE_SEMP_CONFIG).getPassword());

        if(optionalQueueMonitorResponse.isPresent()) {
            numFailedRequestsInARow = 0;
            // Parse the result and return as object
            // TODO - Define customer object instead of using SEMPv2 generated classes?
            Gson gson = new Gson();
            return gson.fromJson(optionalQueueMonitorResponse.get(), SempQueueResponse.class);
        } else {
            numFailedRequestsInARow++;
            // TODO: Make number of requests in a row configurable
            if(numFailedRequestsInARow > 5) {
                throw new IOException("Failed to fetch queue statistics from active broker for 5 separate intervals. Please confirm Broker Semp Configuration.");
            }
            return new SempQueueResponse();
        }
    }

    /**
     * Updates the Active Message VPN to use when fetching queue monitoring statistics
     * @throws URISyntaxException
     * @throws IOException
     */
    public synchronized void updateActiveVpnForForQueueMonitor() throws URISyntaxException, IOException {
        Optional<SempMessageVpnStateResponse> optionalMessageVpnStateResponse = getVpnStateForSempConfig(sempConfigMap.get(ACTIVE_SEMP_CONFIG));
        if(optionalMessageVpnStateResponse.isEmpty() || !optionalMessageVpnStateResponse.get().getData().getState().equals(SEMP_VPN_STATE_UP)) {
            log.info("SempUrl={} -- Unable to fetch Message VPN State from Active SEMP Config. Trying Standby", sempConfigMap.get(ACTIVE_SEMP_CONFIG).getBrokerSempUrl());
            if(sempConfigMap.get(STANDBY_SEMP_CONFIG) != null) {
                optionalMessageVpnStateResponse = getVpnStateForSempConfig(sempConfigMap.get(STANDBY_SEMP_CONFIG));
                if(optionalMessageVpnStateResponse.isPresent() && optionalMessageVpnStateResponse.get().getData().getState().equals(SEMP_VPN_STATE_UP)) {
                    // Update active vpn so that we check it first on the next interval
                    ScalerConfig.SempConfig prevActiveVpn = sempConfigMap.get(ACTIVE_SEMP_CONFIG);
                    sempConfigMap.put(ACTIVE_SEMP_CONFIG, sempConfigMap.get(STANDBY_SEMP_CONFIG));
                    sempConfigMap.put(STANDBY_SEMP_CONFIG, prevActiveVpn);
                } else {
                    // TODO: update to fail after X number of failed attempts in a row
                    log.error("MessageVPN={} -- Neither Message VPN is currently up. Skipping retrieval of Queue Metrics", messageVpnName);
                }
            }
        }
    }

    /**
     * Retrieves the state of the given message vpn. Enables Scaler app to continue running in the event of a broker DR failover
     * @param sempConfig
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    public synchronized Optional<SempMessageVpnStateResponse> getVpnStateForSempConfig(ScalerConfig.SempConfig sempConfig) throws URISyntaxException, IOException {
        Optional<String> optionalVpnStateResponse = getSempResponse(formatVpnStateUrl(sempConfig.getBrokerSempUrl(), messageVpnName), sempConfig.getUsername(), sempConfig.getPassword());

        if(optionalVpnStateResponse.isPresent()) {
            // Parse the result and return as object
            Gson gson = new Gson();
            return Optional.of(gson.fromJson(optionalVpnStateResponse.get(), SempMessageVpnStateResponse.class));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Executes Http GET request for the given url and basic auth params
     * @param urlString
     * @param username
     * @param password
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    public synchronized Optional<String> getSempResponse(String urlString, String username, String password) throws IOException, URISyntaxException {
        HttpURLConnection connection = connectionFactory.createConnection(urlString);

        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "application/json");

        String authHeader = formatHttpBasicAuthHeader(username, password);
        if(authHeader != null ) {
            connection.setRequestProperty("Authorization", authHeader);
        }
        connection.setConnectTimeout(2500);

        int responseCode = connection.getResponseCode();
        if (responseCode < 200 || responseCode > 204 ) {
            // Issue with configuration or the broker service, throw exception to stop execution.
            log.error( "MsgVpn={} -- Call to SEMP responseCode = {}",
                    messageVpnName, responseCode );
            log.error( "MsgVpn={} -- SEMP Response Message: {}",
                    messageVpnName, connection.getResponseMessage() );
            connection.disconnect();
            return Optional.empty();
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

        return Optional.of(content.toString());
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