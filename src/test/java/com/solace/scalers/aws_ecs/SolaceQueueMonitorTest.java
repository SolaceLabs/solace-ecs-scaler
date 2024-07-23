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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SolaceQueueMonitorTest {

    public static final String VPN_STATE_UP_SEMP_RESPONSE = "{\"data\":{\"state\":\"up\"}}";
    public static final String QUEUE_MONITOR_RESPONSE = "{\"data\":{\"averageRxMsgRate\":0,\"averageTxMsgRate\":0,\"msgSpoolUsage\":0,\"msgVpnName\":\"demo2\",\"queueName\":\"ecsQ_customAutoscaling\"},\"collections\":{\"msgs\":{\"count\":0}},\"links\":{},\"meta\":{\"request\":{\"method\":\"GET\",\"uri\":\"https://mr-connection-de86rbn9ibm.messaging.solace.cloud:943/SEMP/v2/monitor/msgVpns/demo2/queues/service-queue-1?select=msgs.count,msgVpnName,queueName,msgSpoolUsage,averageRxMsgRate,averageTxMsgRate\"},\"responseCode\":200}}";


    @InjectMocks
    private SolaceQueueMonitor solaceQueueMonitor;

    @Test
    public void getSempMonitorForQueue_Active() throws Exception {
        String configFile = "src/test/resources/configs/valid-config.yaml";
        ScalerConfig scalerConfig = ScalerConfigParser.validateScalerConfig(ScalerConfigParser.parseScalerConfig(configFile));

        Map<String, ScalerConfig.SempConfig> sempConfigMap = new ConcurrentHashMap<>();
        sempConfigMap.put("active", scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig());

        URLConnectionFactory mockFactory = mock(URLConnectionFactory.class);
        HttpURLConnection vpnStateConnection = mock(HttpURLConnection.class);
        HttpURLConnection queueMonitorConnection = mock(HttpURLConnection.class);


        when(mockFactory.createConnection(SolaceQueueMonitor.formatVpnStateUrl(scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig().getBrokerSempUrl(),scalerConfig.getBrokerConfig().getMsgVpnName()))).thenReturn(vpnStateConnection);
        when(vpnStateConnection.getResponseCode()).thenReturn(200);
        when(vpnStateConnection.getInputStream()).thenReturn(new ByteArrayInputStream(VPN_STATE_UP_SEMP_RESPONSE.getBytes()));

        when(mockFactory.createConnection(SolaceQueueMonitor.formatQueueMonitorUrl(scalerConfig.getBrokerConfig().getActiveMsgVpnSempConfig().getBrokerSempUrl(),scalerConfig.getBrokerConfig().getMsgVpnName(),scalerConfig.getEcsServiceConfig().get(0).getQueueName()))).thenReturn(queueMonitorConnection).thenReturn(queueMonitorConnection);
        when(queueMonitorConnection.getResponseCode()).thenReturn(200);
        when(queueMonitorConnection.getInputStream()).thenReturn(new ByteArrayInputStream(QUEUE_MONITOR_RESPONSE.getBytes()));

        solaceQueueMonitor = new SolaceQueueMonitor(sempConfigMap,scalerConfig.getBrokerConfig().getMsgVpnName(),scalerConfig.getEcsServiceConfig().get(0).getQueueName(),mockFactory);

        SempQueueResponse sempQueueResponse = solaceQueueMonitor.getSempMonitorForQueue();

        assertNotNull(sempQueueResponse);


    }
}
