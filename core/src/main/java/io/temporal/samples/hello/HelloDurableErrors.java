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
import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowNotFoundException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInit;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Sample Temporal Workflow Definition that manages error notifications
 * after a first attempt has failed. This workflow receives a signal to start
 * with the error count.
 * If successful, send it an exit signal to stop waiting and return.
 */
public class HelloDurableErrors {
  // set a time to wait for another signal to come in, e.g.
  // Duration.ofDays(30);
  static final Duration MAX_AWAIT_TIME = Duration.ofMinutes(1);

  static final String TASK_QUEUE = "HelloDurableErrorsTaskQueue";
  static final String WORKFLOW_ID_PREFIX = "DurableErrorsWorkflow";
  static final int MAX_ATTEMPT_COUNT = 7;

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
  public interface DurableErrorWorkflow {
    /**
     * This is the method that is executed when the Workflow Execution is started. The Workflow
     * Execution completes when this method finishes execution.
     */
    @WorkflowMethod
    String manageErrorNotification(String inputMessageID, int initialAttemptCount);

    // Define the workflow attempt count signal
    @SignalMethod
    void setAttemptCount(int attemptCount);

    // Define the workflow attempt count signal
    @SignalMethod
    void signalSuccess();

    // Define the workflow exit signal method. This method is executed when the
    // workflow receives an exit signal.
    @SignalMethod
    void exit();
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
  public interface ErrorManagementActivities {
    String retryMessage(String messageID);

    String confirmSuccess(String messageID);
  }

  /** Simple activity implementation. */
  static class ErrorManagementActivitiesImpl implements ErrorManagementActivities {

    private static final Logger log = LoggerFactory.getLogger(ErrorManagementActivitiesImpl.class);

    // here is where we send the notification
    @Override
    public String retryMessage(String messageID) {
      // here we could call an API or send a message to indicate
      // a retry should be done
      // for now it is mocked for simplicity
      log.info("Triggered Retry for Error Message " + messageID);
      return "Mocked trigger for " + messageID + ": done!";
    }

    // here is where we confirm success of message
    @Override
    public String confirmSuccess(String messageID) {
      // here we could write to a database or system of record
      // to indicate success of the process
      // for now it is mocked
      log.info("Updated success database for Message " + messageID);
      return "Updated tracking system for " + messageID + ": done!";
    }
  }

  // Main workflow method
  public static class DurableErrorWorkflowImpl implements DurableErrorWorkflow {

    private final ErrorManagementActivities activities =
        Workflow.newActivityStub(
            ErrorManagementActivities.class,
            ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(2)).build());

    private static final Logger logger = LoggerFactory.getLogger(DurableErrorWorkflowImpl.class);

    String messageID; // unique key for message to durably trigger
    int attemptCount;
    boolean gotNewAttemptCountSignal;
    boolean exitRequested;
    boolean success;

    // you can optionally define a workflow initializer that runs first and sets up the
    // workflow variables as you desire
    @WorkflowInit
    public DurableErrorWorkflowImpl(String inputMessageID, int initialAttemptCount) {
      messageID = inputMessageID;
      attemptCount = initialAttemptCount;
      gotNewAttemptCountSignal = false;
      exitRequested = false;
      success = false;
    }

    @Override
    public String manageErrorNotification(String inputMessageID, int initialAttemptCount) {

      // wait steadily increasing times as calculated by calculateAwait() for
      // external signals
      do {
        gotNewAttemptCountSignal = false;

        Duration waitTime = calculateAwait(attemptCount);
        boolean timedout =
            !Workflow.await(waitTime, () -> gotNewAttemptCountSignal || exitRequested || success);

        if (success) {
          activities.confirmSuccess(messageID);
          return "Successfully notified, ending workflow";
        }
        if (exitRequested) {
          return "Exit requested, ending workflow";
        }
        if (gotNewAttemptCountSignal) {
          logger.info(
              "Processing Signal for a new Attempt Count, which resets how long we await before triggering retry");
          gotNewAttemptCountSignal = false;
        } else if (timedout) {
          logger.info(
              "After attempt count "
                  + attemptCount
                  + ", didn't get a signal about success."
                  + " Retrying sending error message and increase attempt count.");

          activities.retryMessage(messageID);
          attemptCount++;
          if (attemptCount > MAX_ATTEMPT_COUNT) {
            logger.info("We've attempted the maximum number of tries, returning.");
            // could send an alert here to indicate we hit max attempts count for visibility
            // could set a custom search attribute here to indicate we hit max attempts for
            // visibility
            return "Hit max attempts count, ("
                + MAX_ATTEMPT_COUNT
                + "), no more attempts will be tried.";
          }
        }
      } while (!Workflow.getInfo().isContinueAsNewSuggested());

      logger.info("starting continue as new processing");

      // Create a workflow stub that will be used to continue this workflow as a new
      DurableErrorWorkflow continueAsNew =
          Workflow.newContinueAsNewStub(DurableErrorWorkflow.class);

      // Request that the new run will be invoked by the Temporal system:
      continueAsNew.manageErrorNotification(messageID, attemptCount);

      return "continued as new; results passed to next run";
    }

    // Signal method
    // generally it's bad to call activities from signal handlers
    // but it's ok at low volume of signals
    @Override
    public void setAttemptCount(int newAttemptCount) {
      gotNewAttemptCountSignal = true;
      attemptCount = newAttemptCount;
    }

    // Signal method
    // generally it's bad to call activities from signal handlers
    @Override
    public void signalSuccess() {
      success = true;
    }

    @Override
    public void exit() {
      logger.info("exit signal received");
      exitRequested = true;
    }
  }

  /**
   * With the Workflow and Activities defined, we can now start execution. The main method starts
   * the worker and then the workflow.
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
     * Define the workflow factory. It is used to create workflow workers for a
     * specific task queue.
     */
    WorkerFactory factory = WorkerFactory.newInstance(client);

    /*
     * Define the workflow worker. Workflow workers listen to a defined task queue
     * and process
     * workflows and activities.
     */
    Worker worker = factory.newWorker(TASK_QUEUE);

    /*
     * Register the workflow implementation with the worker.
     * Workflow implementations must be known to the worker at runtime in
     * order to dispatch workflow tasks.
     */
    worker.registerWorkflowImplementationTypes(DurableErrorWorkflowImpl.class);

    /*
     * Register our Activity Types with the Worker. Since Activities are stateless
     * and thread-safe,
     * the Activity Type is a shared instance.
     */
    worker.registerActivitiesImplementations(new ErrorManagementActivitiesImpl());

    /*
     * Start all the workers registered for a specific task queue.
     * The started workers then start polling for workflows and activities.
     */
    factory.start();

    System.out.println("Worker started for task queue: " + TASK_QUEUE);
    boolean demoSignalWithStart = true;
    boolean demoSecondSignal = true;
    boolean demoStartThenSignal = true;
    boolean demoStartThenLetTimeout = true;
    boolean demoStartThenExit = true;

    // we can signal with start the workflow, then tell it that it has succeeded
    // (you can also send multiple signal-with-starts, which are treated just as a signal if this
    // workflow is already started)
    if (demoSignalWithStart) {
      String workflowId = WORKFLOW_ID_PREFIX + "-" + "Customer-12354-demoSignalWithStart";
      WorkflowOptions workflowOptions =
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();
      DurableErrorWorkflow workflow =
          client.newWorkflowStub(DurableErrorWorkflow.class, workflowOptions);

      BatchRequest request = client.newSignalWithStartRequest();
      request.add(workflow::manageErrorNotification, "Error-Message-3451", 1);
      request.add(workflow::setAttemptCount, 1);
      client.signalWithStart(request);
      Thread.sleep(20000); // simulate some delay so the messages are triggered

      workflow.signalSuccess(); // we have succeeded, ok to end!

      // Demonstrate we still can connect to WF and get result using untyped:
      try {
        WorkflowStub untyped = WorkflowStub.fromTyped(workflow);
        // wait for it to finish
        String results = untyped.getResult(String.class);
        System.out.println(results);
      } catch (WorkflowFailedException e) {
        System.out.println("Workflow failed: " + e.getCause().getMessage());
      }
    }

    // we can signal with start the workflow, send a second signal, then tell it that it has
    // succeeded
    if (demoSecondSignal) {
      String workflowId = WORKFLOW_ID_PREFIX + "-" + "Customer-112358-demoSignalWStartThenSignal";
      WorkflowOptions workflowOptions =
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();
      DurableErrorWorkflow workflow =
          client.newWorkflowStub(DurableErrorWorkflow.class, workflowOptions);

      BatchRequest request = client.newSignalWithStartRequest();
      request.add(workflow::manageErrorNotification, "Error-Message-111", 1);
      request.add(workflow::setAttemptCount, 1);
      client.signalWithStart(request);
      Thread.sleep(100); // simulate some delay

      try {
        /*
         * Here we create a new workflow stub using the same workflow id.
         * We do this to demonstrate that to send a signal to an already running
         * workflow you only need to know its workflow id.
         */
        DurableErrorWorkflow workflowById =
            client.newWorkflowStub(DurableErrorWorkflow.class, workflowId);
        // Send the second signal to our workflow to indicate we actually had another failure
        workflowById.setAttemptCount(2);
      } catch (WorkflowNotFoundException e) {
        System.out.println("Workflow not found: " + e.getCause().getMessage());
      }

      Thread.sleep(10000); // simulate some delay to wait on the second attempt

      workflow.signalSuccess(); // we have succeeded, ok to end!

      // Demonstrate we still can connect to WF and get result using untyped:
      try {
        WorkflowStub untyped = WorkflowStub.fromTyped(workflow);
        // wait for it to finish
        String results = untyped.getResult(String.class);
        System.out.println(results);
      } catch (WorkflowFailedException e) {
        System.out.println("Workflow failed: " + e.getCause().getMessage());
      }
    }

    // we can also start and then signal
    if (demoStartThenSignal) {
      String workflowId = WORKFLOW_ID_PREFIX + "-" + "Customer-11235-demoStartThenSignal";
      WorkflowOptions workflowOptions =
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();
      // Create the workflow client stub. It is used to start the workflow execution.
      DurableErrorWorkflow workflow =
          client.newWorkflowStub(DurableErrorWorkflow.class, workflowOptions);

      // Start workflow asynchronously and call its getGreeting workflow method
      WorkflowClient.start(workflow::manageErrorNotification, "Error-Message-11235", 1);

      // After start for Workflowclient returns, the workflow is guaranteed to be
      // started, so we can send a signal to it using the workflow stub.
      // This workflow keeps receiving signals until MAX_ATTEMPT_COUNT, exit signalled, or success
      // is signalled

      // When the workflow is started the workflow will block on a timer for the
      // previously defined conditions

      // Send the first workflow signal
      workflow.setAttemptCount(2);
      Thread.sleep(10000); // simulate some delay to wait on the attempts
      workflow.signalSuccess(); // we have succeeded, ok to end!

      // wait for results
      String results = workflow.manageErrorNotification("Error-Message-11235", 2);
      System.out.println(results);
    }

    // here we start the workflow and then let it try until it quits
    // number of attempts is defined by MAX_ATTEMPT_COUNT
    if (demoStartThenLetTimeout) {
      String workflowId = WORKFLOW_ID_PREFIX + "-" + "Customer-12355-demoStartThenTimeout";
      WorkflowOptions workflowOptions =
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();
      DurableErrorWorkflow workflow =
          client.newWorkflowStub(DurableErrorWorkflow.class, workflowOptions);

      BatchRequest request = client.newSignalWithStartRequest();
      request.add(workflow::manageErrorNotification, "Error-Message-55", 1);
      request.add(workflow::setAttemptCount, 1);
      client.signalWithStart(request);
      Thread.sleep(20); // simulate some delay

      // Demonstrate we still can connect to WF and get result using untyped:
      try {
        WorkflowStub untyped = WorkflowStub.fromTyped(workflow);
        // wait for it to finish
        String results = untyped.getResult(String.class);
        System.out.println(results);
      } catch (WorkflowFailedException e) {
        System.out.println("Workflow failed: " + e.getCause().getMessage());
      }
    }

    // demonstrate telling the workflow to exit in case you just want it to stop
    // but not mark success in system of record
    if (demoStartThenExit) {
      String workflowId = WORKFLOW_ID_PREFIX + "-" + "Customer-123541-demoStartThenExit";
      WorkflowOptions workflowOptions =
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();
      DurableErrorWorkflow workflow =
          client.newWorkflowStub(DurableErrorWorkflow.class, workflowOptions);

      BatchRequest request = client.newSignalWithStartRequest();
      request.add(workflow::manageErrorNotification, "Error-Message-778", 1);
      request.add(workflow::setAttemptCount, 1);
      client.signalWithStart(request);
      Thread.sleep(5000); // simulate some delay to simulate it triggering
      workflow.exit(); // tell it to exit

      // Demonstrate we still can connect to WF and get result using untyped:
      try {
        WorkflowStub untyped = WorkflowStub.fromTyped(workflow);
        // wait for it to finish
        String results = untyped.getResult(String.class);
        System.out.println(results);
      } catch (WorkflowFailedException e) {
        System.out.println("Workflow failed: " + e.getCause().getMessage());
      }
    }

    System.exit(0);
  }

  // here we can define how long to wait
  private static Duration calculateAwait(int attemptCount) {
    Duration awaitDuration;
    switch (attemptCount) {
      case 1:
        awaitDuration = Duration.ofSeconds(5);
        break;
      case 2:
        awaitDuration = Duration.ofSeconds(10);
        break;
      case 3:
        awaitDuration = Duration.ofSeconds(20);
        break;
      default:
        awaitDuration = Duration.ofSeconds(30);
        break;
    }

    return awaitDuration;
  }
}
