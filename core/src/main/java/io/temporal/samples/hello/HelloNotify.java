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

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import java.util.Random;

/*
 * Sample Temporal Workflow Definition that sends notifications
 * Retries are managed with Temporal retry policy and customized by activity error handling
 * We could use Temporal Workflow awaits or timers to manage that as well.
 */
public class HelloNotify {
  static final Duration MAX_RETRY_DURATION = Duration.ofDays(1);
  static final Duration INITIAL_RETRY_DURATION =
      Duration.ofSeconds(5); // for demo purposes keep this quick

  static final String TASK_QUEUE = "HelloNotifyTaskQueue";
  static final String WORKFLOW_ID_PREFIX = "NotifyWorkflow";

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
  public interface NotifyWorkflow {
    /**
     * This is the method that is executed when the Workflow Execution is started. The Workflow
     * Execution completes when this method finishes execution.
     */
    @WorkflowMethod
    String notify(String message);
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
  public interface NotificationActivities {
    String sendNotification(String notification);
  }

  /** Simple activity implementation. */
  static class NotificationActivitiesImpl implements NotificationActivities {

    // here is where we send the notification
    // error sometimes to demonstrate retries
    @Override
    public String sendNotification(String notification) {

      try {
        return sendNotificationAPICall(notification);

      } catch (RuntimeException e) {
        // here is where we can set the duration as desired
        // this overrides the retry interval on the retry policy
        int attempt = Activity.getExecutionContext().getInfo().getAttempt();

        Duration retryDuration = calculateAwait(attempt);

        throw ApplicationFailure.newFailureWithCauseAndDelay(
            "Notification API call failured happened on attempt " + attempt,
            "my_failure_type",
            null,
            retryDuration);
      }
    }
  }

  // Main workflow method
  public static class NotifyWorkflowImpl implements NotifyWorkflow {

    // here is where we can set retry settings
    private final NotificationActivities activities =
        Workflow.newActivityStub(
            NotificationActivities.class,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(2))
                .setRetryOptions(
                    RetryOptions.newBuilder()
                        .setInitialInterval(
                            INITIAL_RETRY_DURATION) // this is overridden by activity error handling
                        // customization, above
                        .setBackoffCoefficient(10)
                        .setMaximumInterval(MAX_RETRY_DURATION)
                        .build())
                .build());

    @Override
    public String notify(String notification) {
      activities.sendNotification(
          notification); // retries managed here by retry policy defined above

      // alternatively you could define max retries as 1
      //   and a series of  Workflow.await(AWAIT_TIME, () ->  exitRequested)
      //   and activities calls to have fine grained control of notifications

      return "Sent Notification [" + notification + "] successfully!";
    }
  }

  /**
   * With the Workflow and Activities defined, we can now start execution. The main method starts
   * the worker and then the workflow. Often the client starter is separated from the worker and is
   * triggered by a message or RPC call but in our sample we will keep them together for simplicity
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
    worker.registerWorkflowImplementationTypes(NotifyWorkflowImpl.class);

    /*
     * Register our Activity Types with the Worker. Since Activities are stateless
     * and thread-safe,
     * the Activity Type is a shared instance.
     */
    worker.registerActivitiesImplementations(new NotificationActivitiesImpl());

    /*
     * Start all the workers registered for a specific task queue.
     * The started workers then start polling for workflows and activities.
     */
    factory.start();

    System.out.println("Worker started for task queue: " + TASK_QUEUE);

    // use a key with business meaning to deduplicate and identify the workflow
    String notificationID = "Customer-Error-11235";
    String workflowId = WORKFLOW_ID_PREFIX + "-Notify-" + notificationID;

    // Create the workflow options
    WorkflowOptions workflowOptions =
        WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();

    NotifyWorkflow workflow = client.newWorkflowStub(NotifyWorkflow.class, workflowOptions);

    // Start workflow synchronously and call its getGreeting workflow method
    String notificationResults = workflow.notify("Important Notification For You");

    // Display workflow execution results
    System.out.println(notificationResults);

    System.exit(0);
  }

  private static String sendNotificationAPICall(String notification) {
    Random random = new Random();

    // Here is where you could call Notification Service
    // but for the sample we will simulate a high chance of error
    // Generate a random number between 0 and 1
    double errorChance = random.nextDouble();
    // Check if the random number is less than 0.5 (50% chance)
    if (errorChance < 0.5) {
      throw new RuntimeException("Notification Failure simulated");
    }
    return "Mocked notification for " + notification + " sent successfully";
  }

  // here we can define how long to wait
  private static Duration calculateAwait(int attemptCount) {
    Duration awaitDuration;
    switch (attemptCount) {
      case 1:
        awaitDuration = INITIAL_RETRY_DURATION;
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
