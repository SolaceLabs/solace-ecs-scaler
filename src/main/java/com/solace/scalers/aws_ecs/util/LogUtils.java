package com.solace.scalers.aws_ecs.util;

import com.solace.scalers.aws_ecs.model.ScalerConfig.EcsServiceConfig;

/**
 * Helper class containing functions to support logging
 */
public class LogUtils {
    
    /**
     * Return Service designation = ECSCluster/ECSService to report on logs
     * @param ecsServiceConfig
     * @return String with service identifier to use in logging entries
     */
    public static String getServiceDesignation( EcsServiceConfig ecsServiceConfig ) {
        return ecsServiceConfig.getEcsCluster() + "/" + ecsServiceConfig.getEcsService();
    }
}
