package com.solace.scalers.aws_ecs.model.util.ecs;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class EcsServiceScalerObservations {

    private long maxMessageCountObservationUp = 0L;
    private long maxAverageMessageRateObservationUp = 0L;
    private long maxMessageCountObservationDown = 0L;
    private long maxAverageMessageRateObservationDown = 0L;
    private long newestObservationTime = 0L;
    private long newestMessageCountObservation = 0L;
    private long newestAverageMessageRateObservation = 0L;

    private boolean foundWindowObservationUp = false;
    private boolean foundWindowObservationDown = false;
    private boolean foundNewObservation = false;
    private boolean computeScaleOut = true;
    private boolean computeScaleIn = true;

}
