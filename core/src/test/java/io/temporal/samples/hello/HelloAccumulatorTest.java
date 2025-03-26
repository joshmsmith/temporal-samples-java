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

import static io.temporal.samples.hello.HelloAccumulator.MAX_AWAIT_TIME;

import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.samples.hello.HelloAccumulator.Greeting;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.TestWorkflowRule;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;

public class HelloAccumulatorTest {

  private TestWorkflowEnvironment testEnv;

  @Rule
  public TestWorkflowRule testWorkflowRule =
      TestWorkflowRule.newBuilder()
          .setWorkflowTypes(HelloAccumulator.AccumulatorWorkflowImpl.class)
          .setActivityImplementations(new HelloAccumulator.GreetingActivitiesImpl())
          .build();

  @Test
  public void testWorkflow() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    long millis = System.currentTimeMillis() - 10000;
    testEnv =
        TestWorkflowEnvironment.newInstance(
            TestEnvironmentOptions.newBuilder().setInitialTimeMillis(millis).build());
    testEnv.start();

    HelloAccumulator.AccumulatorWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                HelloAccumulator.AccumulatorWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId("accumConflict")
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .build());

    WorkflowClient.start(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    Greeting xvxGreeting = new Greeting("XVX Robot", bucket, "1123581321");

    workflow.sendGreeting(xvxGreeting);

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (1)");
    assert results.contains("XVX Robot");

    WorkflowStub stub =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub("accum", Optional.empty(), Optional.empty());
    DescribeWorkflowExecutionRequest describeWorkflowExecutionRequest =
        DescribeWorkflowExecutionRequest.newBuilder()
            .setNamespace(testWorkflowRule.getWorkflowClient().getOptions().getNamespace())
            .setExecution(stub.getExecution())
            .build();
    DescribeWorkflowExecutionResponse resp =
        testWorkflowRule
            .getWorkflowClient()
            .getWorkflowServiceStubs()
            .blockingStub()
            .describeWorkflowExecution(describeWorkflowExecutionRequest);

    WorkflowExecutionInfo workflowExecutionInfo = resp.getWorkflowExecutionInfo();

    com.google.protobuf.Timestamp starttime = workflowExecutionInfo.getStartTime();
    assert (com.google.protobuf.util.Timestamps.toMillis(starttime) < System.currentTimeMillis());
    assert (com.google.protobuf.util.Timestamps.toMillis(starttime) > millis);

    String status = workflowExecutionInfo.getStatus().toString();
    assert ("WORKFLOW_EXECUTION_STATUS_TIMED_OUT".equals(status));

    HelloAccumulator.AccumulatorWorkflow workflowConflict =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                HelloAccumulator.AccumulatorWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowId("accumConflict")
                    .build());

    Greeting starterGreeting = new Greeting("Robby Robot", bucket, "112");
    BatchRequest request = testWorkflowRule.getWorkflowClient().newSignalWithStartRequest();
    request.add(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow::sendGreeting, starterGreeting);
    testWorkflowRule.getWorkflowClient().signalWithStart(request);

    String resultsConflict =
        workflowConflict.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert resultsConflict.contains("Hello (1)");
    assert resultsConflict.contains("Robby Robot");
  }

  @Test
  public void testConflictsWorkflow() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    long millis = System.currentTimeMillis() - 10000;
    testEnv =
        TestWorkflowEnvironment.newInstance(
            TestEnvironmentOptions.newBuilder().setInitialTimeMillis(millis).build());
    testEnv.start();

    HelloAccumulator.AccumulatorWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                HelloAccumulator.AccumulatorWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId("accum")
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowIdReusePolicy(
                        WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
                    .build());

    WorkflowClient.start(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    Greeting xvxGreeting = new Greeting("XVX Robot", bucket, "1123581321");

    workflow.sendGreeting(xvxGreeting);

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (1)");
    assert results.contains("XVX Robot");

    WorkflowStub parentStub =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub("accum", Optional.empty(), Optional.empty());
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

    com.google.protobuf.Timestamp starttime = workflowExecutionInfo.getStartTime();
    assert (com.google.protobuf.util.Timestamps.toMillis(starttime) < System.currentTimeMillis());
    assert (com.google.protobuf.util.Timestamps.toMillis(starttime) > millis);

    String parentStatus = workflowExecutionInfo.getStatus().toString();
    // assertEquals("WORKFLOW_EXECUTION_STATUS_TIMED_OUT", parentStatus);
  }

  @Test
  public void testJustExit() {
    String bucket = "blue";
    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    HelloAccumulator.AccumulatorWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                HelloAccumulator.AccumulatorWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());

    WorkflowClient.start(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    workflow.exit();

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (0)");
    assert !results.contains("Robot");
  }

  @Test
  public void testNoExit() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    HelloAccumulator.AccumulatorWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                HelloAccumulator.AccumulatorWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());

    WorkflowClient.start(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    Greeting xvxGreeting = new Greeting("XVX Robot", bucket, "1123581321");

    workflow.sendGreeting(xvxGreeting);

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (1)");
    assert results.contains("XVX Robot");
  }

  @Test
  public void testMultipleGreetings() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    HelloAccumulator.AccumulatorWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                HelloAccumulator.AccumulatorWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());

    WorkflowClient.start(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    workflow.sendGreeting(new Greeting("XVX Robot", bucket, "1123581321"));
    workflow.sendGreeting(new Greeting("Han Robot", bucket, "112358"));

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (2)");
    assert results.contains("XVX Robot");
    assert results.contains("Han Robot");
  }

  @Test
  public void testContinueAsNew() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    String workflowID = "accumulate-" + bucket;
    HelloAccumulator.AccumulatorWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                HelloAccumulator.AccumulatorWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowId(workflowID)
                    .build());

    WorkflowClient.start(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    String originalRunID = Workflow.getInfo().getRunId();

    // WorkflowStub wfStub =
    //     testWorkflowRule
    //         .getWorkflowClient()
    //         .newUntypedWorkflowStub(workflowID, Optional.empty(), Optional.empty());
    // DescribeWorkflowExecutionRequest describeWorkflowExecutionRequest =
    //     DescribeWorkflowExecutionRequest.newBuilder()
    //         .setNamespace(testWorkflowRule.getWorkflowClient().getOptions().getNamespace())
    //         .setExecution(wfStub.getExecution())
    //         .build();

    // DescribeWorkflowExecutionResponse resp =
    //     testWorkflowRule
    //         .getWorkflowClient()
    //         .getWorkflowServiceStubs()
    //         .blockingStub()
    //         .describeWorkflowExecution(describeWorkflowExecutionRequest);

    // WorkflowExecutionInfo workflowExecutionInfo = resp.getWorkflowExecutionInfo();
    // String runID = workflowExecutionInfo.getFirstRunId();
    // workflowExecutionInfo.getrunID
    // String wfStatus = workflowExecutionInfo.getStatus().toString();
    // assertEquals("WORKFLOW_EXECUTION_STATUS_TIMED_OUT", parentStatus);

    for (int i = 0; i < 2000; i++) {

      Random randomName = new Random();
      workflow.sendGreeting(new Greeting("Robot #" + i, bucket, "key" + i));
    }

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);

    String latestRunID = Workflow.getInfo().getRunId();
    assert !latestRunID.equals(originalRunID);
    assert results.contains("Hello (2000)");
    assert results.contains("Robot #2000");
  }

  @Test
  public void testDuplicateGreetings() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    HelloAccumulator.AccumulatorWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                HelloAccumulator.AccumulatorWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());

    WorkflowClient.start(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    workflow.sendGreeting(new Greeting("XVX Robot", bucket, "1123581321"));
    workflow.sendGreeting(new Greeting("Han Robot", bucket, "1123581321"));

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (1)");
    assert results.contains("XVX Robot");
    assert !results.contains("Han Robot");
  }

  @Test
  public void testWrongBucketGreeting() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    HelloAccumulator.AccumulatorWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                HelloAccumulator.AccumulatorWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());

    WorkflowClient.start(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    workflow.sendGreeting(new Greeting("Bad Robot", "orange", "1123581321"));
    workflow.sendGreeting(new Greeting("XVX Robot", bucket, "11235"));

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (1)");
    assert results.contains("XVX Robot");
    assert !results.contains("Bad Robot");
  }

  @Test
  public void testSignalWithStart() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    WorkflowClient client = testWorkflowRule.getWorkflowClient();
    HelloAccumulator.AccumulatorWorkflow workflow =
        client.newWorkflowStub(
            HelloAccumulator.AccumulatorWorkflow.class,
            WorkflowOptions.newBuilder().setTaskQueue(testWorkflowRule.getTaskQueue()).build());

    Greeting starterGreeting = new Greeting("Robby Robot", bucket, "112");
    BatchRequest request = client.newSignalWithStartRequest();
    request.add(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow::sendGreeting, starterGreeting);
    client.signalWithStart(request);

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (1)");
    assert results.contains("Robby Robot");
  }

  @Test
  public void testWaitTooLongForFirstWorkflow() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    WorkflowClient client = testWorkflowRule.getWorkflowClient();
    HelloAccumulator.AccumulatorWorkflow workflow =
        client.newWorkflowStub(
            HelloAccumulator.AccumulatorWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(testWorkflowRule.getTaskQueue())
                .setWorkflowId(bucket)
                .setWorkflowId("helloacc-blue")
                .build());

    Greeting starterGreeting = new Greeting("Robby Robot", bucket, "112");
    BatchRequest request = client.newSignalWithStartRequest();
    request.add(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow::sendGreeting, starterGreeting);
    client.signalWithStart(request);

    // testEnv.sleep(MAX_AWAIT_TIME.plus(Duration.ofMillis(1))); is not long enough
    // to guarantee the
    // first workflow will end
    testEnv.sleep(MAX_AWAIT_TIME.plus(Duration.ofMillis(100)));

    HelloAccumulator.AccumulatorWorkflow workflow2 =
        client.newWorkflowStub(
            HelloAccumulator.AccumulatorWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(testWorkflowRule.getTaskQueue())
                .setWorkflowId(bucket)
                .setWorkflowId("helloacc-blue")
                .build());

    Greeting secondGreeting = new Greeting("Dave Robot", bucket, "1123");

    request = client.newSignalWithStartRequest();
    request.add(workflow2::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow2::sendGreeting, secondGreeting);
    client.signalWithStart(request);

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (1)");
    assert !results.contains("Robby Robot");
    assert results.contains("Dave Robot");
  }

  @Test
  public void testWaitNotLongEnoughForNewWorkflow() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    WorkflowClient client = testWorkflowRule.getWorkflowClient();
    HelloAccumulator.AccumulatorWorkflow workflow =
        client.newWorkflowStub(
            HelloAccumulator.AccumulatorWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(testWorkflowRule.getTaskQueue())
                .setWorkflowId(bucket)
                .setWorkflowId("helloacc-blue")
                .build());

    Greeting starterGreeting = new Greeting("Robby Robot", bucket, "112");
    BatchRequest request = client.newSignalWithStartRequest();
    request.add(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow::sendGreeting, starterGreeting);
    client.signalWithStart(request);

    testEnv.sleep(MAX_AWAIT_TIME.minus(Duration.ofMillis(1)));

    HelloAccumulator.AccumulatorWorkflow workflow2 =
        client.newWorkflowStub(
            HelloAccumulator.AccumulatorWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(testWorkflowRule.getTaskQueue())
                .setWorkflowId(bucket)
                .setWorkflowId("helloacc-blue")
                .build());

    Greeting secondGreeting = new Greeting("Dave Robot", bucket, "1123");

    request = client.newSignalWithStartRequest();
    request.add(workflow2::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow2::sendGreeting, secondGreeting);
    client.signalWithStart(request);

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (2)");
    assert results.contains("Robby Robot");
    assert results.contains("Dave Robot");
  }

  @Test
  public void testWaitExactlyMAX_TIME() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    WorkflowClient client = testWorkflowRule.getWorkflowClient();
    HelloAccumulator.AccumulatorWorkflow workflow =
        client.newWorkflowStub(
            HelloAccumulator.AccumulatorWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(testWorkflowRule.getTaskQueue())
                .setWorkflowId(bucket)
                .setWorkflowId("helloacc-blue")
                .build());

    Greeting starterGreeting = new Greeting("Robby Robot", bucket, "112");
    BatchRequest request = client.newSignalWithStartRequest();
    request.add(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow::sendGreeting, starterGreeting);
    client.signalWithStart(request);

    testEnv.sleep(MAX_AWAIT_TIME);

    HelloAccumulator.AccumulatorWorkflow workflow2 =
        client.newWorkflowStub(
            HelloAccumulator.AccumulatorWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(testWorkflowRule.getTaskQueue())
                .setWorkflowId(bucket)
                .setWorkflowId("helloacc-blue")
                .build());

    Greeting secondGreeting = new Greeting("Dave Robot", bucket, "1123");

    request = client.newSignalWithStartRequest();
    request.add(workflow2::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow2::sendGreeting, secondGreeting);
    client.signalWithStart(request);

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (2)");
    assert results.contains("Robby Robot");
    assert results.contains("Dave Robot");
  }

  @Test
  public void testSignalAfterExit() {
    String bucket = "blue";

    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    testEnv = testWorkflowRule.getTestEnvironment();
    testEnv.start();

    WorkflowClient client = testWorkflowRule.getWorkflowClient();
    HelloAccumulator.AccumulatorWorkflow workflow =
        client.newWorkflowStub(
            HelloAccumulator.AccumulatorWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(testWorkflowRule.getTaskQueue())
                .setWorkflowId(bucket)
                .setWorkflowId("helloacc-blue")
                .build());

    Greeting starterGreeting = new Greeting("Robby Robot", bucket, "112");
    BatchRequest request = client.newSignalWithStartRequest();
    request.add(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow::sendGreeting, starterGreeting);
    client.signalWithStart(request);

    HelloAccumulator.AccumulatorWorkflow workflow2 =
        client.newWorkflowStub(
            HelloAccumulator.AccumulatorWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(testWorkflowRule.getTaskQueue())
                .setWorkflowId(bucket)
                .setWorkflowId("helloacc-blue")
                .build());

    Greeting secondGreeting = new Greeting("Dave Robot", bucket, "1123");

    request = client.newSignalWithStartRequest();
    request.add(workflow2::accumulateGreetings, bucket, greetingList, allGreetingsSet);
    request.add(workflow2::sendGreeting, secondGreeting);

    // exit signal the workflow we signaled-to-start
    workflow.exit();

    // try to signal with start the workflow
    client.signalWithStart(request);

    String results = workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);
    assert results.contains("Hello (2)");
    assert results.contains("Robby Robot");
    assert results.contains("Dave Robot");
  }
}
