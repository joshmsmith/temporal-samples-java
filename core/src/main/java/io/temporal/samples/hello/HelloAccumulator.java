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

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityOptions;
import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowNotFoundException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateHandle;
import io.temporal.client.WorkflowUpdateStage;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInit;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Sample Temporal Workflow Definition that accumulates events.
 * This sample implements the Accumulator Pattern: collect many meaningful
 *  things that need to be collected and worked on together, such as
 *  all payments for an account, or all account updates by account.
 *
 * This sample models robots being created throughout the time period,
 *  groups them by what color they are, and greets all the robots
 *  of a color at the end.
 *
 * A new Workflow is created per grouping. Workflows continue as new as needed.
 *  A sample Activity at the end is given, and you could add an Activity to
 *  process individual events in the processGreeting() method.
 */
public class HelloAccumulator {
  // set a time to wait for another Signal to come in, e.g.
  // Duration.ofDays(30);
  static final Duration MAX_AWAIT_TIME = Duration.ofMinutes(1);

  static final String TASK_QUEUE = "HelloAccumulatorTaskQueue";
  static final String WORKFLOW_ID_PREFIX = "HelloAccumulatorWorkflow";

  public static class Greeting implements Serializable {
    String greetingText;
    String bucket;
    String greetingKey;

    public String getGreetingText() {
      return greetingText;
    }

    public void setGreetingText(String greetingText) {
      this.greetingText = greetingText;
    }

    public String getBucket() {
      return bucket;
    }

    public void setBucket(String bucket) {
      this.bucket = bucket;
    }

    public String getGreetingKey() {
      return greetingKey;
    }

    public void setGreetingKey(String greetingKey) {
      this.greetingKey = greetingKey;
    }

    public Greeting(String greetingText, String bucket, String greetingKey) {
      this.greetingText = greetingText;
      this.bucket = bucket;
      this.greetingKey = greetingKey;
    }

    public Greeting() {}

    @Override
    public String toString() {
      return "Greeting [greetingText="
          + greetingText
          + ", bucket="
          + bucket
          + ", greetingKey="
          + greetingKey
          + "]";
    }
  }

  /**
   * The Workflow Definition's Interface must contain one method annotated with @WorkflowMethod.
   *
   * <p>Workflow Definitions should not contain any heavyweight computations, non-deterministic
   * code, network calls, database operations, etc. Those things should be handled by the
   * Activities.
   *
   * @see io.temporal.workflow.WorkflowInterface
   * @see io.temporal.workflow.WorkflowMethod
   */
  @WorkflowInterface
  public interface AccumulatorWorkflow {
    /**
     * This is the method that is executed when the Workflow Execution is started. The Workflow
     * Execution completes when this method finishes execution.
     */
    @WorkflowMethod
    String accumulateGreetings(
        String bucketKey, Deque<Greeting> greetings, Set<String> allGreetingsSet);

    // Define the Workflow sendGreeting Signal method. This method is executed when
    // the Workflow receives a greeting Signal.
    @SignalMethod
    void sendGreeting(Greeting greeting);

    // Define the Workflow exit Signal method. This method is executed when the
    // Workflow receives an exit Signal.
    @SignalMethod
    void exit();

    // Define the Workflow greeting update method. This method is executed when
    // the Workflow receives a greeting update.
    @UpdateMethod
    boolean sendAndValidateGreeting(Greeting greeting);

    // Update validators are optional
    // Validators must return void and accept the same argument types as the handler
    @UpdateValidatorMethod(updateName = "sendAndValidateGreeting")
    void greetingValidator(Greeting greeting);
  }

  /**
   * This is the Activity Definition's Interface. Activities are building blocks of any Temporal
   * Workflow and contain any business logic that could perform long running computation, network
   * calls, etc.
   *
   * <p>Annotating Activity Definition methods with @ActivityMethod is optional.
   *
   * @see io.temporal.activity.ActivityInterface
   * @see io.temporal.activity.ActivityMethod
   */
  @ActivityInterface
  public interface GreetingActivities {
    String composeGreeting(Deque<Greeting> greetings);
  }

  /** Simple Activity implementation. */
  static class GreetingActivitiesImpl implements GreetingActivities {

    // here is where we process all of the Signals together
    @Override
    public String composeGreeting(Deque<Greeting> greetings) {
      List<String> greetingList =
          greetings.stream().map(u -> u.greetingText).collect(Collectors.toList());
      return "Hello (" + greetingList.size() + ") robots: " + greetingList + "!";
    }
  }

  // Main Workflow method
  public static class AccumulatorWorkflowImpl implements AccumulatorWorkflow {

    private final GreetingActivities activities =
        Workflow.newActivityStub(
            GreetingActivities.class,
            ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(2)).build());

    private static final Logger logger = LoggerFactory.getLogger(AccumulatorWorkflowImpl.class);
    String bucketKey;
    ArrayDeque<Greeting> greetings;
    HashSet<String> allGreetingsSet;
    boolean exitRequested = false;
    ArrayDeque<Greeting> unprocessedGreetings = new ArrayDeque<Greeting>();

    @WorkflowInit
    public AccumulatorWorkflowImpl(
        String bucketKeyInput, Deque<Greeting> greetingsInput, Set<String> allGreetingsSetInput) {
      bucketKey = bucketKeyInput;
      greetings = new ArrayDeque<Greeting>();
      allGreetingsSet = new HashSet<String>();
      greetings.addAll(greetingsInput);
      allGreetingsSet.addAll(allGreetingsSetInput);
    }

    @Override
    public String accumulateGreetings(
        String bucketKeyInput, Deque<Greeting> greetingsInput, Set<String> allGreetingsSetInput) {

      // If you want to wait for a fixed amount of time instead of a time after a
      // message
      // as this does now, you might want to check out
      // ../../updatabletimer

      // Main Workflow Loop:
      // - wait for Signals to come in
      // - every time a Signal comes in, process all Signals then wait again for MAX_AWAIT_TIME
      //     - if your use case is with a long timer and you need less responsive Signal processing,
      //     - you may want to just wait for a fixed time and then process Signals, that means less
      // timers
      //     - you can do that by taking out the unprocessedGreetings.isEmpty() in the await() below
      // - if time runs out, and there are no messages, process them all and exit
      // - if exit Signal is received, process any remaining Signals and exit (no more waiting)
      do {
        boolean conditionMet =
            Workflow.await(MAX_AWAIT_TIME, () -> !unprocessedGreetings.isEmpty() || exitRequested);
        boolean timedout = !conditionMet;

        while (!unprocessedGreetings.isEmpty()) {
          processGreeting(unprocessedGreetings.removeFirst());
        }

        if (exitRequested || timedout) {
          String greetEveryone = processGreetings(greetings);

          if (Workflow.isEveryHandlerFinished() && unprocessedGreetings.isEmpty()) {
            logger.info("Greeting queue is still empty, ok to exit.");
            return greetEveryone;
          } else {
            // todo test this with new handler checks
            // you can get here if you send a Signal after an exit, causing rollback just
            // after the last processed Activity
            logger.warn("Greeting queue not empty, looping.");
          }
        }
      } while (!unprocessedGreetings.isEmpty() || !Workflow.getInfo().isContinueAsNewSuggested());

      logger.info("Starting continue as new processing.");

      // Create a Workflow stub that will be used to continue this Workflow as a new
      AccumulatorWorkflow continueAsNew = Workflow.newContinueAsNewStub(AccumulatorWorkflow.class);

      // Request that the new run will be invoked by the Temporal system:
      continueAsNew.accumulateGreetings(bucketKey, greetings, allGreetingsSet);
      // todo test this with the new handlers check
      // this could be improved in the future with the are_handlers_finished API. For
      // now if a Signal comes in
      // after this, it will fail the Workflow task and retry handling the new
      // signal(s)

      return "Continued as new; results passed to next run.";
    }

    // Here is where we can process individual Signals as they come in.
    // It's ok to call Activities here.
    // This also validates an individual greeting:
    // - check for duplicates
    // - check for correct bucket
    // - check out the Update feature which can validate before accepting an update
    // - validations are not necessary here if using Update
    public void processGreeting(Greeting greeting) {
      if (greeting == null) {
        logger.warn("Greeting is null:" + greeting);
        return;
      }

      // This just ignores incorrect buckets - you can use workflowupdate to validate
      // and reject bad bucket requests if needed
      if (!greeting.bucket.equals(bucketKey)) {
        logger.warn("Wrong bucket, something is wrong with your Signal processing: " + greeting);
        return;
      }

      if (!allGreetingsSet.add(greeting.greetingKey)) {
        logger.info("Duplicate Signal event: " + greeting.greetingKey);
        return;
      }

      // Include any desired event processing Activity here
      greetings.add(greeting);
    }

    private String processGreetings(Deque<Greeting> greetings) {
      logger.info("Composing greetings for: " + greetings);
      return activities.composeGreeting(greetings);
    }

    // Signal method
    // Keep it simple, these should generally be fast and not call Activities
    // See  https://docs.temporal.io/develop/java/message-passing#blocking-handlers
    @Override
    public void sendGreeting(Greeting greeting) {
      logger.info("received greeting-" + greeting);
      unprocessedGreetings.add(greeting);
    }

    @Override
    public void exit() {
      logger.info("Exit Signal received");
      exitRequested = true;
    }

    // Update Method Implementation
    // called when the Workflow receives a greeting update.
    @Override
    public boolean sendAndValidateGreeting(Greeting greeting) {
      logger.info("received greeting-" + greeting);
      unprocessedGreetings.add(greeting);
      return true;
    }

    // Define Update validator.
    // Update validators are optional.
    // Validators must return void and accept the same argument types as the handler.
    // To reject an Update, throw an exception of any type in the validator.
    // Without a validator, Updates are always accepted.
    @Override
    public void greetingValidator(Greeting greeting) {

      if (greeting == null) {
        throw new RuntimeException("Greeting is null:" + greeting);
      }

      if (!greeting.bucket.equals(bucketKey)) {
        throw new RuntimeException(
            "Wrong bucket, something is wrong with your update sending: " + greeting);
      }

      if (!allGreetingsSet.add(greeting.greetingKey)) {
        throw new RuntimeException("Duplicate greeting: " + greeting.greetingKey);
      }
    }
  }

  /**
   * With the Workflow and Activities defined, we can now start execution. The main method starts
   * the worker and then the Workflow.
   */
  public static void main(String[] args) throws Exception {

    // Get a Workflow service stub.
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();

    /*
     * Get a Workflow service client which can be used to start, Signal, and Query
     * Workflow Executions.
     */
    WorkflowClient client = WorkflowClient.newInstance(service);
    client.getWorkflowServiceStubs().healthCheck();

    /*
     * Define the Workflow factory. It is used to create Workflow workers for a
     * specific task queue.
     */
    WorkerFactory factory = WorkerFactory.newInstance(client);

    /*
     * Define the Workflow worker. Workflow workers listen to a defined task queue
     * and process
     * Workflows and Activities.
     */
    Worker worker = factory.newWorker(TASK_QUEUE);

    /*
     * Register the Workflow implementation with the worker.
     * Workflow implementations must be known to the worker at runtime in
     * order to dispatch Workflow tasks.
     */
    worker.registerWorkflowImplementationTypes(AccumulatorWorkflowImpl.class);

    /*
     * Register our Activity Types with the Worker. Since Activities are stateless
     * and thread-safe,
     * the Activity Type is a shared instance.
     */
    worker.registerActivitiesImplementations(new GreetingActivitiesImpl());

    /*
     * Start all the workers registered for a specific task queue.
     * The started workers then start polling for Workflows and Activities.
     */
    factory.start();

    System.out.println("Worker started for task queue: " + TASK_QUEUE);

    demonstrateSignalAccumulation(client);
    demonstrateUpdateAccumulation(client);

    System.exit(0);
  }

  private static void demonstrateSignalAccumulation(WorkflowClient client)
      throws InterruptedException {
    // setup which demonstrations to run
    // by default it will run an accumulation with a few (20) Signals
    // to a set of 4 buckets with Signal To Start
    boolean demonstrateContinueAsNew = false;

    // configure Signal edge cases to test
    boolean demonstrateSignalAfterWorkflowExit = true;
    boolean demonstrateSignalAfterExitSignal = !demonstrateSignalAfterWorkflowExit;
    boolean demonstrateDuplicate = true;
    boolean demonstrateInvalidBucket = true;

    // setup to send Signals
    String bucket = "blue";
    String workflowId = WORKFLOW_ID_PREFIX + "-" + bucket;
    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    String greetingKey = "key-";
    String greetingText = "Robby Robot";
    Greeting starterGreeting = new Greeting(greetingText, bucket, greetingKey);
    final String[] buckets = {"red", "blue", "green", "yellow"};
    final String[] names = {"Genghis Khan", "Missy", "Bill", "Ted", "Rufus", "Abe"};

    // Create the Workflow options
    WorkflowOptions workflowOptions =
        WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();
    AccumulatorWorkflow workflow =
        client.newWorkflowStub(AccumulatorWorkflow.class, workflowOptions);

    // send many Signals to start several Workflows
    int max_signals = 20;

    if (demonstrateContinueAsNew) max_signals = 5000;
    for (int i = 0; i < max_signals; i++) {
      Random randomBucket = new Random();
      int bucketIndex = randomBucket.nextInt(buckets.length);
      bucket = buckets[bucketIndex];
      starterGreeting.setBucket(bucket);
      Thread.sleep(20); // wait a bit to simulate the distribution of events over time

      workflowId = WORKFLOW_ID_PREFIX + "-" + bucket;

      Random randomName = new Random();
      int nameIndex = randomName.nextInt(names.length);
      starterGreeting.setGreetingText(names[nameIndex] + " Robot");

      workflowOptions =
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();

      // Create the Workflow client stub. It is used to start the Workflow execution.
      workflow = client.newWorkflowStub(AccumulatorWorkflow.class, workflowOptions);

      BatchRequest request = client.newSignalWithStartRequest();
      starterGreeting.greetingKey = greetingKey + i;
      request.add(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);
      request.add(workflow::sendGreeting, starterGreeting);
      client.signalWithStart(request);
    }

    // Demonstrate we still can connect to WF and get result using untyped:
    if (max_signals > 0) {
      WorkflowStub untyped = WorkflowStub.fromTyped(workflow);

      // wait for it to finish
      try {
        String greeting = untyped.getResult(String.class);
        printWorkflowStatus(client, workflowId);
        System.out.println("Greeting: " + greeting);
      } catch (WorkflowFailedException e) {
        System.out.println("Workflow failed: " + e.getCause().getMessage());
        printWorkflowStatus(client, workflowId);
      }
    }

    // set Workflow parameters
    bucket = "purple";
    greetingList = new ArrayDeque<Greeting>();
    allGreetingsSet = new HashSet<String>();
    workflowId = WORKFLOW_ID_PREFIX + "-" + bucket;
    workflowOptions =
        WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();

    starterGreeting = new Greeting("Suzy Robot", bucket, "11235813");

    // Create the Workflow client stub. It is used to start the Workflow execution.
    AccumulatorWorkflow workflowSync =
        client.newWorkflowStub(AccumulatorWorkflow.class, workflowOptions);

    // Start Workflow asynchronously and call its getGreeting Workflow method
    WorkflowClient.start(workflowSync::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    // After start for accumulateGreetings returns, the Workflow is guaranteed to be
    // started, so we can send a Signal to it using the Workflow stub.
    // This Workflow keeps receiving Signals until exit is called or the timer
    // finishes with no
    // Signals

    // When the Workflow is started the accumulateGreetings will block for the
    // previously defined conditions
    // Send the first Workflow Signal
    workflowSync.sendGreeting(starterGreeting);

    // Test sending an exit, waiting for Workflow exit, then sending a Signal.
    // This will trigger a WorkflowNotFoundException if using the same Workflow
    // handle
    if (demonstrateSignalAfterWorkflowExit) {
      workflowSync.exit();
      String greetingsAfterExit =
          workflowSync.accumulateGreetings(bucket, greetingList, allGreetingsSet);
      System.out.println(greetingsAfterExit);
    }

    // todo update/test this with Signal  Functionality and handler checks
    // Test sending an exit, not waiting for Workflow to exit, then sending a Signal
    // this demonstrates Temporal history rollback
    // see https://community.temporal.io/t/continueasnew-signals/1008/7
    if (demonstrateSignalAfterExitSignal) {
      workflowSync.exit();
    }

    // Test sending more Signals after Workflow exit
    try {
      // send a second Workflow Signal
      Greeting janeGreeting = new Greeting("Jane Robot", bucket, "112358132134");
      workflowSync.sendGreeting(janeGreeting);

      if (demonstrateInvalidBucket) {
        // send a third Signal with an incorrect bucket - this will be ignored
        // can use Workflow update to validate and reject a request if needed
        workflowSync.sendGreeting(new Greeting("Sally Robot", "taupe", "112358132134"));
      }

      if (demonstrateDuplicate) {
        // intentionally send a duplicate Signal
        workflowSync.sendGreeting(janeGreeting);
      }

      if (!demonstrateSignalAfterWorkflowExit) {
        // wait for results if we haven't waited for them yet
        String greetingsAfterExit =
            workflowSync.accumulateGreetings(bucket, greetingList, allGreetingsSet);
        System.out.println(greetingsAfterExit);
      }
    } catch (WorkflowNotFoundException e) {
      System.out.println("Workflow not found - this is intentional: " + e.getCause().getMessage());
      printWorkflowStatus(client, workflowId);
    }

    try {
      /*
       * Here we create a new Workflow stub using the same Workflow id.
       * We do this to demonstrate that to send a Signal to an already running
       * Workflow you only need to know its Workflow id.
       */
      AccumulatorWorkflow workflowById =
          client.newWorkflowStub(AccumulatorWorkflow.class, workflowId);

      Greeting laterGreeting = new Greeting("XVX Robot", bucket, "1123581321");
      // Send the second Signal to our Workflow
      workflowById.sendGreeting(laterGreeting);

      // Now let's send our exit Signal to the Workflow
      workflowById.exit();

      /*
       * We now call our accumulateGreetings Workflow method synchronously after our
       * Workflow has started.
       * This reconnects our workflowById Workflow stub to the existing Workflow and
       * blocks until a result is available. Note that this behavior assumes that
       * WorkflowOptions
       * are not configured with WorkflowIdReusePolicy.AllowDuplicate. If they were,
       * this call would fail
       * with the WorkflowExecutionAlreadyStartedException exception.
       * You can use the policy to force Workflows for a new time period, e.g. a
       * collection day, to have a new Workflow ID.
       */

      String greetings = workflowById.accumulateGreetings(bucket, greetingList, allGreetingsSet);

      // Print our results for greetings which were sent by Signals
      System.out.println(greetings);
    } catch (WorkflowNotFoundException e) {
      System.out.println("Workflow not found - this is intentional: " + e.getCause().getMessage());
      printWorkflowStatus(client, workflowId);
    }

    /*
     * Here we try to send the Signals as start to demonstrate that after a Workflow
     * exited and Signals failed to send
     * we can send Signals to a new Workflow
     * (This also waits for Workflows to end so we can safely continue.)
     */
    if (demonstrateSignalAfterWorkflowExit) {
      greetingList = new ArrayDeque<Greeting>();
      allGreetingsSet = new HashSet<String>();
      workflowId = WORKFLOW_ID_PREFIX + "-" + bucket;

      Greeting laterGreeting = new Greeting("Final Robot", bucket, "1123");
      // Send the second Signal to our Workflow

      workflowOptions =
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();

      // Create the Workflow client stub. It is used to start the Workflow execution.
      workflow = client.newWorkflowStub(AccumulatorWorkflow.class, workflowOptions);

      BatchRequest request = client.newSignalWithStartRequest();
      request.add(workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet);
      request.add(workflow::sendGreeting, laterGreeting);
      client.signalWithStart(request);

      printWorkflowStatus(client, workflowId);

      String greetingsAfterExit =
          workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);

      // Print our results for greetings which were sent by Signals
      System.out.println(greetingsAfterExit);

      printWorkflowStatus(client, workflowId);

      while (getWorkflowStatus(client, workflowId).equals("WORKFLOW_EXECUTION_STATUS_RUNNING")) {

        System.out.println("Workflow still running ");
        Thread.sleep(1000);
      }
    }
  }

  private static void demonstrateUpdateAccumulation(WorkflowClient client)
      throws InterruptedException, ExecutionException {
    // setup which demonstrations to run
    // by default it will run an accumulation with a few (20) updates
    // to a set of 4 buckets with Update With Start
    boolean demonstrateContinueAsNew = false;

    // configure Update edge cases to test
    boolean demonstrateUpdateAfterWorkflowExit = true;
    boolean demonstrateUpdateAfterExitSignal = !demonstrateUpdateAfterWorkflowExit;
    boolean demonstrateDuplicate = true;
    boolean demonstrateInvalidBucket = true;

    // setup to send Updates
    String bucket = "blue";
    String workflowId = WORKFLOW_ID_PREFIX + "-" + bucket;
    ArrayDeque<Greeting> greetingList = new ArrayDeque<Greeting>();
    HashSet<String> allGreetingsSet = new HashSet<String>();
    String greetingKey = "key-";
    String greetingText = "Robby Robot";
    Greeting starterGreeting = new Greeting(greetingText, bucket, greetingKey);
    final String[] buckets = {"red", "blue", "green", "yellow"};
    final String[] names = {"Genghis Khan", "Missy", "Bill", "Ted", "Rufus", "Abe"};

    // Create the Workflow options
    WorkflowOptions workflowOptions =
        WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();
    AccumulatorWorkflow workflow =
        client.newWorkflowStub(AccumulatorWorkflow.class, workflowOptions);

    // send many Updates to start several Workflows
    int max_updates = 20;

    if (demonstrateContinueAsNew) max_updates = 5000;
    for (int i = 0; i < max_updates; i++) {
      Random randomBucket = new Random();
      int bucketIndex = randomBucket.nextInt(buckets.length);
      bucket = buckets[bucketIndex];
      starterGreeting.setBucket(bucket);
      Thread.sleep(20); // wait a bit to simulate the distribution of events over time

      workflowId = WORKFLOW_ID_PREFIX + "-" + bucket;

      Random randomName = new Random();
      int nameIndex = randomName.nextInt(names.length);
      starterGreeting.setGreetingText(names[nameIndex] + " Robot");
      starterGreeting.greetingKey = greetingKey + i;

      workflowOptions =
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();

      // Create the Workflow client stub. It is used to start the Workflow execution.
      workflow = client.newWorkflowStub(AccumulatorWorkflow.class, workflowOptions);

      UpdateWithStartWorkflowOperation<Boolean> updateOp =
          UpdateWithStartWorkflowOperation.newBuilder(
                  workflow::sendAndValidateGreeting, starterGreeting)
              .setWaitForStage(WorkflowUpdateStage.COMPLETED) // Wait for Update to complete
              .build();
      WorkflowUpdateHandle<Boolean> updateHandle =
          WorkflowClient.updateWithStart(
              workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet, updateOp);

      Boolean updateResult = updateHandle.getResultAsync().get();
      // todo probably remove this
      System.out.println("Update result: " + updateResult);
    }

    // Demonstrate we still can connect to WF and get result using untyped:
    if (max_updates > 0) {
      WorkflowStub untyped = WorkflowStub.fromTyped(workflow);

      // wait for it to finish
      try {
        String greeting = untyped.getResult(String.class);
        printWorkflowStatus(client, workflowId);
        System.out.println("Greeting: " + greeting);
      } catch (WorkflowFailedException e) {
        System.out.println("Workflow failed: " + e.getCause().getMessage());
        printWorkflowStatus(client, workflowId);
      }
    }

    // set Workflow parameters
    bucket = "purple";
    greetingList = new ArrayDeque<Greeting>();
    allGreetingsSet = new HashSet<String>();
    workflowId = WORKFLOW_ID_PREFIX + "-" + bucket;
    workflowOptions =
        WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();

    starterGreeting = new Greeting("Suzy Robot", bucket, "11235813");

    // Create the Workflow client stub. It is used to start the Workflow execution.
    AccumulatorWorkflow workflowSync =
        client.newWorkflowStub(AccumulatorWorkflow.class, workflowOptions);

    // Start Workflow asynchronously and call its getGreeting Workflow method
    WorkflowClient.start(workflowSync::accumulateGreetings, bucket, greetingList, allGreetingsSet);

    // After start for accumulateGreetings returns, the Workflow is guaranteed to be
    // started, so we can send an update to it using the Workflow stub.
    // This Workflow keeps receiving updates until exit is called or the timer
    // finishes with no updates

    // When the Workflow is started the accumulateGreetings will block for the
    // previously defined conditions
    // Send the first Workflow update
    workflowSync.sendAndValidateGreeting(starterGreeting);

    // Test sending an exit, waiting for Workflow exit, then sending a Update.
    // This will trigger a WorkflowNotFoundException if using the same Workflow
    // handle
    if (demonstrateUpdateAfterWorkflowExit) {
      workflowSync.exit();
      String greetingsAfterExit =
          workflowSync.accumulateGreetings(bucket, greetingList, allGreetingsSet);
      System.out.println(greetingsAfterExit);
    }

    // todo update/test this with Update Functionality & wait for handlers
    // Test sending an exit, not waiting for Workflow to exit, then sending a Update
    // this demonstrates Temporal history rollback
    // see https://community.temporal.io/t/continueasnew-signals/1008/7
    if (demonstrateUpdateAfterExitSignal) {
      workflowSync.exit();
    }

    // Test sending more Updates after Workflow exit
    try {
      // send a second Workflow Update
      Greeting janeGreeting = new Greeting("Jane Robot", bucket, "112358132134");
      workflowSync.sendGreeting(janeGreeting);

      if (demonstrateInvalidBucket) {
        // send a third Update with an incorrect bucket - this will be caught by Workflow validation
        // and rejected
        workflowSync.sendAndValidateGreeting(new Greeting("Sally Robot", "taupe", "112358132134"));
      }

      if (demonstrateDuplicate) {
        // intentionally send a duplicate Update  - this will be caught by Workflow validation and
        // rejected
        workflowSync.sendAndValidateGreeting(janeGreeting);
      }

      if (!demonstrateUpdateAfterWorkflowExit) {
        // wait for results if we haven't waited for them yet
        String greetingsAfterExit =
            workflowSync.accumulateGreetings(bucket, greetingList, allGreetingsSet);
        System.out.println(greetingsAfterExit);
      }
    } catch (WorkflowNotFoundException e) {
      System.out.println("Workflow not found - this is intentional: " + e.getCause().getMessage());
      printWorkflowStatus(client, workflowId);
    }

    try {
      /*
       * Here we create a new Workflow stub using the same Workflow id.
       * We do this to demonstrate that to send a Update to an already running
       * Workflow you only need to know its Workflow id.
       */
      AccumulatorWorkflow workflowById =
          client.newWorkflowStub(AccumulatorWorkflow.class, workflowId);

      Greeting laterGreeting = new Greeting("XVX Robot", bucket, "1123581321");
      // Send the second Signal to our workflow
      workflowById.sendAndValidateGreeting(laterGreeting);

      // Now let's send our exit Signal to the Workflow
      workflowById.exit();

      /*
       * We now call our accumulateGreetings Workflow method synchronously after our
       * Workflow has started.
       * This reconnects our workflowById Workflow stub to the existing Workflow and
       * blocks until a result is available. Note that this behavior assumes that
       * WorkflowOptions
       * are not configured with WorkflowIdReusePolicy.AllowDuplicate. If they were,
       * this call would fail
       * with the WorkflowExecutionAlreadyStartedException exception.
       * You can use the policy to force Workflows for a new time period, e.g. a
       * collection day, to have a new Workflow ID.
       */

      String greetings = workflowById.accumulateGreetings(bucket, greetingList, allGreetingsSet);

      // Print our results for greetings which were sent by Updates
      System.out.println(greetings);
    } catch (WorkflowNotFoundException e) {
      System.out.println("Workflow not found - this is intentional: " + e.getCause().getMessage());
      printWorkflowStatus(client, workflowId);
    }

    // todo test this with new update functionality
    /*
     * Here we try to send the Updates as start to demonstrate that after a Workflow
     * exited and Updates failed to send  we can send Updates to a new Workflow
     */
    if (demonstrateUpdateAfterWorkflowExit) {
      greetingList = new ArrayDeque<Greeting>();
      allGreetingsSet = new HashSet<String>();
      workflowId = WORKFLOW_ID_PREFIX + "-" + bucket;

      Greeting laterGreeting = new Greeting("Final Robot", bucket, "1123");
      // Send the second Update to our Workflow
      workflowOptions =
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();

      // Create the Workflow client stub. It is used to start the Workflow execution.
      workflow = client.newWorkflowStub(AccumulatorWorkflow.class, workflowOptions);

      UpdateWithStartWorkflowOperation<Boolean> updateOp =
          UpdateWithStartWorkflowOperation.newBuilder(
                  workflow::sendAndValidateGreeting, laterGreeting)
              .setWaitForStage(WorkflowUpdateStage.COMPLETED) // Wait for Update to complete
              .build();
      WorkflowUpdateHandle<Boolean> updateHandle =
          WorkflowClient.updateWithStart(
              workflow::accumulateGreetings, bucket, greetingList, allGreetingsSet, updateOp);

      Boolean updateResult = updateHandle.getResultAsync().get();
      // todo probably remove this
      System.out.println("Update result: " + updateResult);

      printWorkflowStatus(client, workflowId);

      String greetingsAfterExit =
          workflow.accumulateGreetings(bucket, greetingList, allGreetingsSet);

      // Print our results for greetings which were sent by Updates
      System.out.println(greetingsAfterExit);

      printWorkflowStatus(client, workflowId);

      while (getWorkflowStatus(client, workflowId).equals("WORKFLOW_EXECUTION_STATUS_RUNNING")) {

        System.out.println("Workflow still running ");
        Thread.sleep(1000);
      }
    }
  }

  private static void printWorkflowStatus(WorkflowClient client, String workflowId) {
    System.out.println("Workflow Status: " + getWorkflowStatus(client, workflowId));
  }

  private static String getWorkflowStatus(WorkflowClient client, String workflowId) {
    WorkflowStub existingUntyped =
        client.newUntypedWorkflowStub(workflowId, Optional.empty(), Optional.empty());
    DescribeWorkflowExecutionRequest describeWorkflowExecutionRequest =
        DescribeWorkflowExecutionRequest.newBuilder()
            .setNamespace(client.getOptions().getNamespace())
            .setExecution(existingUntyped.getExecution())
            .build();

    DescribeWorkflowExecutionResponse resp =
        client
            .getWorkflowServiceStubs()
            .blockingStub()
            .describeWorkflowExecution(describeWorkflowExecutionRequest);

    WorkflowExecutionInfo workflowExecutionInfo = resp.getWorkflowExecutionInfo();
    return workflowExecutionInfo.getStatus().toString();
  }
}
