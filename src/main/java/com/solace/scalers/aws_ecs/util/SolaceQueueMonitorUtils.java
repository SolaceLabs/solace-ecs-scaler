package com.solace.scalers.aws_ecs.util;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import com.solace.scalers.aws_ecs.EcsServiceScaler;
import com.solace.scalers.aws_ecs.SolaceQueueMonitor;
import com.solace.scalers.aws_ecs.model.ScalerConfig.BrokerConfig;
import com.solace.scalers.aws_ecs.model.ScalerConfig.EcsServiceConfig;
import com.solace.scalers.aws_ecs.model.semp_v2.SempQueueResponse;

/**
 * Helper class to support Solace Queue Monitoring via SEMPv2
 */
public class SolaceQueueMonitorUtils {
    
    /**
     * Factory method to create SolaceQueueMonitor Object from ScalerConfig objects
     * @param brokerConfig
     * @param ecsServiceConfig
     * @return
     * @throws MalformedURLException
     */
    public static SolaceQueueMonitor createSolaceQueueMonitorWithBasicAuth( 
                                            BrokerConfig brokerConfig, 
                                            EcsServiceConfig ecsServiceConfig ) throws MalformedURLException
    {
        SolaceQueueMonitor solaceQueueMonitor = new SolaceQueueMonitor( 
                                    SolaceQueueMonitor.formatUrl(
                                        brokerConfig.getBrokerSempUrl(), 
                                        brokerConfig.getMsgVpnName(), 
                                        ecsServiceConfig.getQueueName()
                                    ), 
                                    ecsServiceConfig.getQueueName() );

        solaceQueueMonitor.setUsername( brokerConfig.getUsername() );
        solaceQueueMonitor.setPassword( brokerConfig.getPassword() );

        return solaceQueueMonitor;
    }

    /**
     * Consolidated method to take the SEMPv2 response from queue monitoring operation and store
     * to corresponding EcsServiceScaler object as a Map 
     * @param msgVpnQueueResponse
     * @return
     */
    public static Map<String, Long> getQueueMetricsFromQueueResponse( SempQueueResponse sempQueueResponse ) {

        Map<String, Long> metrics = new HashMap<>( 3 );
        metrics.put( EcsServiceScaler.METRIC_MSG_COUNT, getMessageCountFromQueueResponse(sempQueueResponse) );
        metrics.put( EcsServiceScaler.METRIC_AVG_RX_RATE, getMessageReceiveRateFromQueueResponse(sempQueueResponse) );
        metrics.put( EcsServiceScaler.METRIC_SPOOL_USAGE, getMessageSpoolUsageFromQueueResponse(sempQueueResponse ));
        
        return metrics;
    }

    /**
     * Method to extract queue messageCount from SEMPv2 response
     * @param msgVpnQueueResponse
     * @return
     */
    public static Long getMessageCountFromQueueResponse( SempQueueResponse sempQueueResponse ) {
        try {
            return sempQueueResponse.getCollections().getMsgs().getCount();
        } catch ( Exception exc ) {
        }
        return null;
    }

    /**
     * Method to extract queue averageRxMsgRate from SEMPv2 response
     * @param msgVpnQueueResponse
     * @return
     */
    public static Long getMessageReceiveRateFromQueueResponse( SempQueueResponse sempQueueResponse ) {
        try {
            return sempQueueResponse.getData().getAverageRxMsgRate();
        } catch ( Exception exc ) {
        }
        return null;
    }

    /**
     * Method to extract queue mesgSpoolUsage from SEMPv2 response
     * @param msgVpnQueueResponse
     * @return
     */
    public static Long getMessageSpoolUsageFromQueueResponse( SempQueueResponse sempQueueResponse ) {
        try {
            return sempQueueResponse.getData().getMsgSpoolUsage();
        } catch ( Exception exc ) {
        }
        return null;
    }
}
