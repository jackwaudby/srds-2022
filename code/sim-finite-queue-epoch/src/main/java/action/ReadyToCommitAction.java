package action;

import event.ReadyToCommitEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.EpochState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.Rand;

import java.util.Objects;

public class ReadyToCommitAction
{
    private final static Logger LOGGER = Logger.getLogger( ReadyToCommitAction.class.getName() );

    public static void ready( ReadyToCommitEvent event, Cluster cluster, Config config, EventList eventList, Rand rand )
    {

        var currentEpoch = cluster.getCurrentEpoch();
        var thisEventTime = event.getEventTime();

        if ( Objects.equals( config.getAlgorithm(), "multi" ) )
        {
            // compute commit groups
            cluster.computeCommitGroups();
            // mark groups with crashed nodes as failed
            cluster.transitionNodesInCrashedGroupsToFailed();

            var operationalCommitGroups = cluster.getNumberOfOperationalCommitGroups();

            if ( operationalCommitGroups > 0 )
            {
                cluster.setCurrentEpochState( EpochState.COMMITTING );
                Common.generateCommitOperationCompletionEvent( eventList, rand, thisEventTime, currentEpoch );
            }
            else
            {
                cluster.setCurrentEpochState( EpochState.ABORTING );
                Common.generateAbortCompletionEvent( thisEventTime, rand, eventList, currentEpoch );
            }
        }
        else
        {
            cluster.setCurrentEpochState( EpochState.COMMITTING );
            Common.generateCommitOperationCompletionEvent( eventList, rand, thisEventTime, currentEpoch );
        }
    }
}
