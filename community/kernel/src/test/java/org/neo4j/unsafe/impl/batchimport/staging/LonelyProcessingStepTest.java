/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.unsafe.impl.batchimport.Configuration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LonelyProcessingStepTest
{
    @ClassRule
    public static SuppressOutput mute = SuppressOutput.suppressAll();

    private final ExecutorService executors = Executors.newCachedThreadPool();

    @After
    public void tearDown() throws Exception
    {
        executors.shutdown();
        executors.awaitTermination( 1, TimeUnit.MINUTES );
    }

    @Test
    public void issuePanicBeforeCompletionOnError()
    {
        List<Step<?>> stepsPipeline = new ArrayList<>();
        BinaryLatch endOfUpstreamLatch = new BinaryLatch();
        FaultyLonelyProcessingStepTest faultyStep =
                new FaultyLonelyProcessingStepTest( stepsPipeline, endOfUpstreamLatch );
        stepsPipeline.add( faultyStep );

        faultyStep.receive( 1, null );

        endOfUpstreamLatch.await();

        assertTrue( "On upstream end step should be already on panic in case of exception",
                faultyStep.isPanicOnEndUpstream() );
        assertTrue( faultyStep.isPanic() );
        assertFalse( faultyStep.stillWorking() );
        assertTrue( faultyStep.isCompleted() );
    }

    private class FaultyLonelyProcessingStepTest extends LonelyProcessingStep
    {
        private final BinaryLatch endOfUpstreamLatch;
        private volatile boolean panicOnEndUpstream;

        FaultyLonelyProcessingStepTest( List<Step<?>> pipeLine, BinaryLatch endOfUpstreamLatch )
        {
            super( new StageExecution( "Faulty", null, Configuration.DEFAULT, pipeLine, 0 ),
                    "Faulty", Configuration.DEFAULT );
            this.endOfUpstreamLatch = endOfUpstreamLatch;
        }

        @Override
        protected void process()
        {
            throw new RuntimeException( "Process exception" );
        }

        @Override
        protected void startProcessing( Runnable runnable )
        {
            executors.submit( runnable );
        }

        @Override
        public void endOfUpstream()
        {
            panicOnEndUpstream = isPanic();
            super.endOfUpstream();
            endOfUpstreamLatch.release();
        }

        public boolean isPanicOnEndUpstream()
        {
            return panicOnEndUpstream;
        }
    }
}
