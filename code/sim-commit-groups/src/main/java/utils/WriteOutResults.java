package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteOutResults
{

    public static void writeOutResults( Config config, Metrics metrics, double propDist )
    {

        String[] headers = {"n", "a", "mu", "avCommitGroupsPerEpoch", "propDist"};

        StringBuilder headerStringBuilder = new StringBuilder();
        for ( String header : headers )
        {
            headerStringBuilder.append( header ).append( "," );
        }
        String headerString = headerStringBuilder.toString();
        if ( headerString.length() > 0 ) // remove trailing comma
        {
            headerString = headerString.substring( 0, headerString.length() - 1 );
        }

        var n = config.getClusterSize(); // cluster size
        var a = config.getEpochTimeoutInMillis(); // epoch timeout
        var mu = config.getTransactionServiceRateInMillis(); // transaction service rate
        var commitGroupsPerEpoch = metrics.getAverageNumberOfCommitGroupsPerEpoch();

        String out = String.format( "%s,%s,%s,%.4f,%s", n, a, mu, commitGroupsPerEpoch, propDist );

        BufferedWriter outputStream = null;
        FileWriter fileWriter;
        try
        {
            File file = new File( "results.csv" );
            if ( !file.exists() )
            {
                fileWriter = new FileWriter( file, true );
                outputStream = new BufferedWriter( fileWriter );
                outputStream.append( headerString );
                outputStream.append( "\n" );
                outputStream.append( out );
                outputStream.append( "\n" );
            }
            else
            {
                fileWriter = new FileWriter( file, true );
                outputStream = new BufferedWriter( fileWriter );
                try
                {
                    outputStream.append( out );
                    outputStream.append( "\n" );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        finally
        {
            if ( outputStream != null )
            {
                try
                {
                    outputStream.flush();
                    outputStream.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }
    }
}