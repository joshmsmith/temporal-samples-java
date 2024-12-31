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

import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.samples.hello.HelloChildSignal.GreetingChild;
import io.temporal.samples.hello.HelloChildSignal.GreetingChildImpl;
import io.temporal.samples.hello.HelloChildSignal.GreetingWorkflow;
import io.temporal.samples.hello.HelloChildSignal.GreetingWorkflowImpl;
import io.temporal.testing.TestWorkflowRule;
import java.time.Duration;
import java.util.Optional;
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

    // start parent
    WorkflowClient.start(workflow::getGreeting, "World");

    // interesting that you can't find the child here, you have to wait for parent to finish

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
  public void testChildAwaitTimeoutForSignal() {
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
    // skip time: wait for parent to start children or you won't find the child in the next step
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMinutes(1));

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
  public void testParentSignaling() {
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

    // signal parent so children can start
    workflow.sendGreeting("asdf");

    // skip time so child workflows start - without this sleep children won't be started yet
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMinutes(1));

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

    // start parent
    WorkflowClient.start(workflow::getGreeting, "World");

    // wait for children to start - without this you won't find the child workflow below
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMinutes(1));

    // signal parent
    workflow.sendGreeting("asdf");

    GreetingChild childWorkflow =
        testWorkflowRule.getWorkflowClient().newWorkflowStub(GreetingChild.class, "Child1");
    // signal child

    childWorkflow.sendGreeting("oranges");

    String childResultFromUntyped =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub("Child1")
            .getResult(String.class);
    String childResultFromTyped = childWorkflow.composeGreeting("Hello2", "World");

    // block for results from Parent
    String greeting = workflow.getGreeting("World");
    // verify results
    // assertEquals(childResult, "yikes no signal");

    assertEquals(childResultFromUntyped, "Hello1 World!");
    assertEquals(childResultFromTyped, "Hello1 World!");

    assertEquals("Hello2 World!", greeting);

    testWorkflowRule.getTestEnvironment().shutdown();
  }

  @Test
  public void testChildSignalAfterParentWFTimeout() {
    testWorkflowRule
        .getWorker()
        .registerWorkflowImplementationTypes(GreetingWorkflowImpl.class, GreetingChildImpl.class);
    testWorkflowRule.getTestEnvironment().start();

    String parentWorkflowID = "Parent";
    // Get a workflow stub using the same task queue the worker uses.
    GreetingWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                GreetingWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowId(parentWorkflowID)
                    .setWorkflowExecutionTimeout(Duration.ofMinutes(55))
                    .build());

    // start parent
    WorkflowClient.start(workflow::getGreeting, "World");

    // signal parent
    workflow.sendGreeting("asdf");

    // wait for parent to time out
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

    WorkflowStub parentStub =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub(parentWorkflowID, Optional.empty(), Optional.empty());
    DescribeWorkflowExecutionRequest describeWorkflowExecutionRequest =
        DescribeWorkflowExecutionRequest.newBuilder()
            .setNamespace(testWorkflowRule.getWorkflowClient().getOptions().getNamespace())
            .setExecution(parentStub.getExecution())
            .build();

    DescribeWorkflowExecutionResponse resp =
        testWorkflowRule
            .getWorkflowClient()
            .getWorkflowServiceStubs()
            .blockingStub()
            .describeWorkflowExecution(describeWorkflowExecutionRequest);

    WorkflowExecutionInfo workflowExecutionInfo = resp.getWorkflowExecutionInfo();
    String parentStatus = workflowExecutionInfo.getStatus().toString();
    assertEquals("WORKFLOW_EXECUTION_STATUS_TIMED_OUT", parentStatus);

    testWorkflowRule.getTestEnvironment().shutdown();
  }
}
