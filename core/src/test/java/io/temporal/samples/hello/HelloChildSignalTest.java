/*
 *  Copyright (c) 2020 Temporal Technologies, Inc. All Rights Reserved
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.samples.hello;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.samples.hello.HelloChildSignal.GreetingChild;
import io.temporal.samples.hello.HelloChildSignal.GreetingChildImpl;
import io.temporal.samples.hello.HelloChildSignal.GreetingWorkflow;
import io.temporal.samples.hello.HelloChildSignal.GreetingWorkflowImpl;
import io.temporal.testing.TestWorkflowRule;
import java.time.Duration;
import org.junit.Rule;
import org.junit.Test;

/** Unit test for {@link HelloChildSignal}. Doesn't use an external Temporal service. */
public class HelloChildSignalTest {

  @Rule
  public TestWorkflowRule testWorkflowRule =
      TestWorkflowRule.newBuilder().setDoNotStart(true).build();

  @Test
  public void testChildResults() {
    testWorkflowRule
        .getWorker()
        .registerWorkflowImplementationTypes(GreetingWorkflowImpl.class, GreetingChildImpl.class);
    testWorkflowRule.getTestEnvironment().start();

    // Get a workflow stub using the same task queue the worker uses.
    GreetingWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                GreetingWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());
    // notes from Block
    // problem 1: start parent, 1 child waits in catch for an hour but env.sleep
    // env.sleep() doesn't skip time just sleeps

    // start parent
    WorkflowClient.start(workflow::getGreeting, "World");

    // interesting that you can't find the child here, you have to wait for parent

    // block for results from Parent
    String greeting = workflow.getGreeting("World");

    // get child results
    String childResult =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub("Child1")
            .getResult(String.class);

    // verify results
    assertEquals(childResult, "yikes no signal");

    assertEquals("Hello2 World!", greeting);

    testWorkflowRule.getTestEnvironment().shutdown();
  }

  @Test
  public void testChildAwaitTimeout() {
    testWorkflowRule
        .getWorker()
        .registerWorkflowImplementationTypes(GreetingWorkflowImpl.class, GreetingChildImpl.class);
    testWorkflowRule.getTestEnvironment().start();

    // Get a workflow stub using the same task queue the worker uses.
    GreetingWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                GreetingWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());
    // notes from Block
    // problem 1: start parent, 1 child waits in catch for an hour but env.sleep
    // env.sleep() doesn't skip time just sleeps

    // start parent
    WorkflowClient.start(workflow::getGreeting, "World");
    // wait
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMinutes(50));

    String childResult =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub("Child1")
            .getResult(String.class);

    // block for results from Parent
    String greeting = workflow.getGreeting("World");
    // verify results
    assertEquals(childResult, "yikes no signal");

    assertEquals("Hello2 World!", greeting);

    testWorkflowRule.getTestEnvironment().shutdown();
  }

  @Test
  public void testChildSignalParent() {
    testWorkflowRule
        .getWorker()
        .registerWorkflowImplementationTypes(GreetingWorkflowImpl.class, GreetingChildImpl.class);
    testWorkflowRule.getTestEnvironment().start();

    // Get a workflow stub using the same task queue the worker uses.
    GreetingWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                GreetingWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());

    // start parent
    WorkflowClient.start(workflow::getGreeting, "World");

    // skip time so child start
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMinutes(20));

    // signal parent so children can start
    workflow.sendGreeting("asdf");

    String childResult =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub("Child1")
            .getResult(String.class);

    // signal parent again
    workflow.sendGreeting("asdf2");

    // block for results from Parent
    String greeting = workflow.getGreeting("World");

    // verify results
    assertEquals(childResult, "yikes no signal");

    assertEquals("Hello2 World!", greeting);

    testWorkflowRule.getTestEnvironment().shutdown();
  }

  @Test
  public void testChildSignal() {
    testWorkflowRule
        .getWorker()
        .registerWorkflowImplementationTypes(GreetingWorkflowImpl.class, GreetingChildImpl.class);
    testWorkflowRule.getTestEnvironment().start();

    // Get a workflow stub using the same task queue the worker uses.
    GreetingWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                GreetingWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());

    // notes from Block

    // problem 2: try to find children, signal one, get results from both, see what happens
    // getting workflow not found exception

    // start parent
    WorkflowClient.start(workflow::getGreeting, "World");

    // wait
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMinutes(50));

    // signal parent
    workflow.sendGreeting("asdf");

    // signal child
    testWorkflowRule
        .getWorkflowClient()
        .newWorkflowStub(GreetingChild.class, "Child1")
        .sendGreeting("oranges");

    String childResult =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub("Child1")
            .getResult(String.class);

    // block for results from Parent
    String greeting = workflow.getGreeting("World");
    // verify results
    // assertEquals(childResult, "yikes no signal");

    assertEquals(childResult, "Hello1 World!");

    assertEquals("Hello2 World!", greeting);

    testWorkflowRule.getTestEnvironment().shutdown();
  }

  @Test
  public void testChildAfterParentWFTimeout() {
    testWorkflowRule
        .getWorker()
        .registerWorkflowImplementationTypes(GreetingWorkflowImpl.class, GreetingChildImpl.class);
    testWorkflowRule.getTestEnvironment().start();

    // Get a workflow stub using the same task queue the worker uses.
    GreetingWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                GreetingWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowExecutionTimeout(Duration.ofMinutes(55))
                    .build());

    // notes from Block

    // problem 3: parent workflow times out, try to find children, signal one, get results from
    // both, see what happens
    // getting workflow not found exception

    // start parent
    WorkflowClient.start(workflow::getGreeting, "World");

    // signal parent
    workflow.sendGreeting("asdf");

    // wait for parent to time
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMinutes(56));
    // signal child
    testWorkflowRule
        .getWorkflowClient()
        .newWorkflowStub(GreetingChild.class, "Child1")
        .sendGreeting("oranges");

    String childResult =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub("Child1")
            .getResult(String.class);
            assertEquals(childResult, "Hello1 World!");

    
    // block for results from Parent
    String greeting = workflow.getGreeting("World");


    assertEquals("Hello2 World!", greeting);

    testWorkflowRule.getTestEnvironment().shutdown();
  }

  //   @Test
  //   public void testMockedChild() {
  //
  // testWorkflowRule.getWorker().registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);

  //     // As new mock is created on each workflow task the only last one is useful to verify
  // calls.
  //     AtomicReference<GreetingChild> lastChildMock = new AtomicReference<>();
  //     // Factory is called to create a new workflow object on each workflow task.
  //     testWorkflowRule
  //         .getWorker()
  //         .registerWorkflowImplementationFactory(
  //             GreetingChild.class,
  //             () -> {
  //               GreetingChild child = mock(GreetingChild.class);
  //               when(child.composeGreeting("Hello", "World")).thenReturn("Hello World!");
  //               lastChildMock.set(child);
  //               return child;
  //             });

  //     testWorkflowRule.getTestEnvironment().start();

  //     // Get a workflow stub using the same task queue the worker uses.
  //     GreetingWorkflow workflow =
  //         testWorkflowRule
  //             .getWorkflowClient()
  //             .newWorkflowStub(
  //                 GreetingWorkflow.class,
  //
  // WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());
  //     // Execute a workflow waiting for it to complete.
  //     String greeting = workflow.getGreeting("World");
  //     assertEquals("Hello World!", greeting);
  //     GreetingChild mock = lastChildMock.get();
  //     verify(mock).composeGreeting(eq("Hello"), eq("World"));

  //     testWorkflowRule.getTestEnvironment().shutdown();
  //   }
}
