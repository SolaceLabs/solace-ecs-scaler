package com.solace.scalers.aws_ecs;

import com.solace.scalers.aws_ecs.model.ScalerConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfigParser;
import com.solace.scalers.aws_ecs.model.semp_v2.SempQueueResponse;
import com.solace.scalers.aws_ecs.http.URLConnectionFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;


import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SolaceQueueMonitorTest {

    public static final String VPN_STATE_RESPONSE_TEMPLATE = "{\"data\":{\"state\":\"%s\"}}";
    public static final String QUEUE_MONITOR_RESPONSE_TEMPLATE = "{\"data\":{\"averageRxMsgRate\":0,\"averageTxMsgRate\":0,\"msgSpoolUsage\":0,\"msgVpnName\":\"demo2\",\"queueName\":\"ecsQ_customAutoscaling\"},\"collections\":{\"msgs\":{\"count\":0}},\"links\":{},\"meta\":{\"request\":{\"method\":\"GET\",\"uri\":\"%s/SEMP/v2/monitor/msgVpns/demo2/queues/service-queue-1?select=msgs.count,msgVpnName,queueName,msgSpoolUsage,averageRxMsgRate,averageTxMsgRate\"},\"responseCode\":200}}";


    @InjectMocks
    private SolaceQueueMonitor solaceQueueMonitor;
    @Mock
    URLConnectionFactory mockFactory;
    @Mock
    HttpURLConnection activeVpnStateConnection;
    @Mock
    HttpURLConnection standbyVpnStateConnection;
    @Mock
    HttpURLConnection queueMonitorConnection;

    Map<String, ScalerConfig.SempConfig> sempConfigMap = new ConcurrentHashMap<>();

    ScalerConfig scalerConfig = new ScalerConfig();

    @Before
    public void setUp() throws Exception {
        String configFile = "src/test/resources/configs/valid-config.yaml";
        scalerConfig = ScalerConfigParser.validateScalerConfig(ScalerConfigParser.parseScalerConfig(configFile));
        sempConfigMap.put("active", scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig());
        sempConfigMap.put("standby", scalerConfig.getBrokerConfig().getStandbyMsgVpnSempConfig());
    }

    @Test
    public void getSempMonitorForQueue_Active() throws Exception {
        // Mock requests for Active Broker VPN Status
        when(mockFactory.createConnection(SolaceQueueMonitor.formatVpnStateUrl(scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig().getBrokerSempUrl(),scalerConfig.getBrokerConfig().getMsgVpnName()))).thenReturn(activeVpnStateConnection);
        when(activeVpnStateConnection.getResponseCode()).thenReturn(200);
        when(activeVpnStateConnection.getInputStream()).thenReturn(new ByteArrayInputStream(formatVpnStateResponse("up").getBytes()));

        // Mock request for Queue Monitor Request on the Active Broker
        when(mockFactory.createConnection(SolaceQueueMonitor.formatQueueMonitorUrl(scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig().getBrokerSempUrl(),scalerConfig.getBrokerConfig().getMsgVpnName(),scalerConfig.getEcsServiceConfig().get(0).getQueueName()))).thenReturn(queueMonitorConnection);
        when(queueMonitorConnection.getResponseCode()).thenReturn(200);
        when(queueMonitorConnection.getInputStream()).thenReturn(new ByteArrayInputStream(formatQueueMonitorResponse(scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig().getBrokerSempUrl()).getBytes()));

        solaceQueueMonitor = new SolaceQueueMonitor(sempConfigMap,scalerConfig.getBrokerConfig().getMsgVpnName(),scalerConfig.getEcsServiceConfig().get(0).getQueueName(),mockFactory);

        SempQueueResponse sempQueueResponse = solaceQueueMonitor.getSempMonitorForQueue();

        assertNotNull(sempQueueResponse);
        assertEquals("demo2", sempQueueResponse.getData().getMsgVpnName());
        // Confirm that the original active broker is still set as the active broker in the SempConfigMap
        assertEquals(scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig().getBrokerSempUrl(), solaceQueueMonitor.getSempConfigMap().get("active").getBrokerSempUrl());
    }

    @Test
    public void getSempMonitorForQueue_Standby() throws Exception {
        // Mock Request for Active Broker VPN Status - Down
        when(mockFactory.createConnection(SolaceQueueMonitor.formatVpnStateUrl(scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig().getBrokerSempUrl(),scalerConfig.getBrokerConfig().getMsgVpnName()))).thenReturn(activeVpnStateConnection);
        when(activeVpnStateConnection.getResponseCode()).thenReturn(200);
        when(activeVpnStateConnection.getInputStream()).thenReturn(new ByteArrayInputStream(formatVpnStateResponse("down").getBytes()));
        // Mock Request for Standby Broker VPN Status - Up
        when(mockFactory.createConnection(SolaceQueueMonitor.formatVpnStateUrl(scalerConfig.getBrokerConfig().getStandbyMsgVpnSempConfig().getBrokerSempUrl(),scalerConfig.getBrokerConfig().getMsgVpnName()))).thenReturn(standbyVpnStateConnection);
        when(standbyVpnStateConnection.getResponseCode()).thenReturn(200);
        when(standbyVpnStateConnection.getInputStream()).thenReturn(new ByteArrayInputStream(formatVpnStateResponse("up").getBytes()));
        // Mock request for the Queue Monitor response on the Standby Broker
        when(mockFactory.createConnection(SolaceQueueMonitor.formatQueueMonitorUrl(scalerConfig.getBrokerConfig().getStandbyMsgVpnSempConfig().getBrokerSempUrl(),scalerConfig.getBrokerConfig().getMsgVpnName(),scalerConfig.getEcsServiceConfig().get(0).getQueueName()))).thenReturn(queueMonitorConnection);
        when(queueMonitorConnection.getResponseCode()).thenReturn(200);
        when(queueMonitorConnection.getInputStream()).thenReturn(new ByteArrayInputStream(formatQueueMonitorResponse(scalerConfig.getBrokerConfig().getStandbyMsgVpnSempConfig().getBrokerSempUrl()).getBytes()));

        solaceQueueMonitor = new SolaceQueueMonitor(sempConfigMap,scalerConfig.getBrokerConfig().getMsgVpnName(),scalerConfig.getEcsServiceConfig().get(0).getQueueName(),mockFactory);

        SempQueueResponse sempQueueResponse = solaceQueueMonitor.getSempMonitorForQueue();

        assertNotNull(sempQueueResponse);
        assertEquals("demo2", sempQueueResponse.getData().getMsgVpnName());
        // Confirm that the original standby broker is now the active broker for the given solaceQueueMonitor
        // This occurs after VPN state request for the original active broker return down
        assertEquals(scalerConfig.getBrokerConfig().getStandbyMsgVpnSempConfig().getBrokerSempUrl(), solaceQueueMonitor.getSempConfigMap().get("active").getBrokerSempUrl());
    }

    private String formatVpnStateResponse(String status) {
        return VPN_STATE_RESPONSE_TEMPLATE.formatted(status);
    }

    private String formatQueueMonitorResponse(String sempUrl) {
        return QUEUE_MONITOR_RESPONSE_TEMPLATE.formatted(sempUrl);
    }
}
