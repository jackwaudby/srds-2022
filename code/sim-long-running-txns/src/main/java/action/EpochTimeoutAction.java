package action;

import event.EpochTimeoutEvent;

import state.Cluster;
import state.EpochState;
import state.NodeState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.Rand;

import java.util.Objects;

public class EpochTimeoutAction
{
    public static void timeout( EpochTimeoutEvent event, Cluster cluster, Config config, EventList eventList, Rand rand )
    {
        var currentEpoch = event.getEpoch();
        var thisEventTime = event.getEventTime();

        // failures are detected at epoch timeout events
        if ( cluster.epochContainsFailureEvent() )
        {
            // multi: transition to wait and form commit groups
            if ( Objects.equals( config.getAlgorithm(), "multi" ) )
            {
                cluster.setCurrentEpochState( EpochState.WAITING );
            }
            // single: immediately abort this epoch
            else
            {
                cluster.setCurrentEpochState( EpochState.WAITING );

//                // for each operational node record a lost job
//                for ( int i = 0; i < config.getClusterSize(); i++ )
//                {
//                    if ( cluster.getNodeState( i ) == NodeState.OPERATIONAL )
//                    {
//                        cluster.incInFlightJobsLost( i );
//                    }
//                }
//
//                Common.transitionToAborting( cluster, eventList, rand, thisEventTime, currentEpoch );
            }
        }
        else
        {
            cluster.setCurrentEpochState( EpochState.WAITING );
        }
    }
}
