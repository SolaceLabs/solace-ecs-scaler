package com.solace.scalers.aws_ecs.model.util.ecs;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class EcsServiceScalerDesiredReplicaTargets {
    private Integer desiredScaleInTarget;
    private Integer desiredScaleOutTarget;
}
