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

package io.temporal.samples.earlyreturn;

import io.temporal.client.UpdateWithStartWorkflowOperation;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateHandle;
import io.temporal.client.WorkflowUpdateStage;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class EarlyReturnClient {
  private static final String TASK_QUEUE = "EarlyReturnTaskQueue";
  private static final String WORKFLOW_ID_PREFIX = "early-return-workflow-";
  private static final int MAX_ATTEMPTS = 100;

  public static void main(String[] args) {
    WorkflowClient client = setupWorkflowClient();
    runWorkflowWithUpdateWithStartNoDelay(client);
    runWorkflowWithUpdateWithStart(client);
    runWorkflowWithUpdateWithStartMoreDelay(client);
  }

  // Set up the WorkflowClient
  public static WorkflowClient setupWorkflowClient() {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    return WorkflowClient.newInstance(service);
  }

  // Run workflow using 'updateWithStart'
  private static void runWorkflowWithUpdateWithStart(WorkflowClient client) {
    TransactionRequest txRequest =
        new TransactionRequest(
            "Bob", "Alice",
            1000); // Change this amount to a negative number to have initTransaction fail

    long totalwfruntime = 0;
    long totalearlyreturnruntime = 0;
    for (int i = 0; i < MAX_ATTEMPTS; i++) {
      TxResult updateResult = null;
      try {
        WorkflowOptions options = buildWorkflowOptionsSomeDelay();
        TransactionWorkflow workflow = client.newWorkflowStub(TransactionWorkflow.class, options);

        // System.out.println("Starting workflow with UpdateWithStart");

        UpdateWithStartWorkflowOperation<TxResult> updateOp =
            UpdateWithStartWorkflowOperation.newBuilder(workflow::returnInitResult)
                .setWaitForStage(WorkflowUpdateStage.COMPLETED) // Wait for update to complete
                .build();
        long starttime = System.currentTimeMillis();

        WorkflowUpdateHandle<TxResult> updateHandle =
            WorkflowClient.updateWithStart(workflow::processTransaction, txRequest, updateOp);

        updateResult = updateHandle.getResultAsync().get();
        long earlyreturnendtime = System.currentTimeMillis();
        totalearlyreturnruntime += earlyreturnendtime - starttime;

        // System.out.println(
        //     "Workflow initialized with result: "
        //         + updateResult.getStatus()
        //         + " (transactionId: "
        //         + updateResult.getTransactionId()
        //         + ")");

        TxResult result = WorkflowStub.fromTyped(workflow).getResult(TxResult.class);
        long wfendtime = System.currentTimeMillis();
        totalwfruntime += wfendtime - starttime;

        if (result == null) { // please stop
          System.out.println(
              "Workflow completed with result: "
                  + result.getStatus()
                  + ", update status: "
                  + updateResult.getStatus()
                  + " (transactionId: "
                  + result.getTransactionId()
                  + ")");
        }
      } catch (Exception e) {
        System.err.println("Transaction initialization failed: " + e.getMessage());
      }
    }

    long averagewfruntime = totalwfruntime / MAX_ATTEMPTS;
    long averageearlyreturnruntime = totalearlyreturnruntime / MAX_ATTEMPTS;
    System.out.println("Latency results with Some Delay over [" + MAX_ATTEMPTS + "] attempts.");
    System.out.println(
        " - Average Early Return response time: " + averageearlyreturnruntime + " milliseconds");
    System.out.println(" - Average Workflow response time: " + averagewfruntime + " milliseconds");
  }

  // Run workflow using 'updateWithStart'
  private static void runWorkflowWithUpdateWithStartNoDelay(WorkflowClient client) {
    TransactionRequest txRequest =
        new TransactionRequest(
            "Bob", "Alice",
            1000); // Change this amount to a negative number to have initTransaction fail

    long totalwfruntime = 0;
    long totalearlyreturnruntime = 0;
    for (int i = 0; i < MAX_ATTEMPTS; i++) {
      TxResult updateResult = null;
      try {
        WorkflowOptions options = buildWorkflowOptionsNoDelay();
        TransactionWorkflowNoDelay workflow =
            client.newWorkflowStub(TransactionWorkflowNoDelay.class, options);

        // System.out.println("Starting workflow with UpdateWithStart");

        UpdateWithStartWorkflowOperation<TxResult> updateOp =
            UpdateWithStartWorkflowOperation.newBuilder(workflow::returnInitResult)
                .setWaitForStage(WorkflowUpdateStage.COMPLETED) // Wait for update to complete
                .build();
        long starttime = System.currentTimeMillis();

        WorkflowUpdateHandle<TxResult> updateHandle =
            WorkflowClient.updateWithStart(workflow::processTransaction, txRequest, updateOp);

        updateResult = updateHandle.getResultAsync().get();
        long earlyreturnendtime = System.currentTimeMillis();
        totalearlyreturnruntime += earlyreturnendtime - starttime;

        // System.out.println(
        //     "Workflow initialized with result: "
        //         + updateResult.getStatus()
        //         + " (transactionId: "
        //         + updateResult.getTransactionId()
        //         + ")");

        TxResult result = WorkflowStub.fromTyped(workflow).getResult(TxResult.class);
        long wfendtime = System.currentTimeMillis();
        totalwfruntime += wfendtime - starttime;

        if (result == null) { // please stop
          System.out.println(
              "Workflow completed with result: "
                  + result.getStatus()
                  + ", update status: "
                  + updateResult.getStatus()
                  + " (transactionId: "
                  + result.getTransactionId()
                  + ")");
        }
      } catch (Exception e) {
        System.err.println("Transaction initialization failed: " + e.getMessage());
      }
    }

    long averagewfruntime = totalwfruntime / MAX_ATTEMPTS;
    long averageearlyreturnruntime = totalearlyreturnruntime / MAX_ATTEMPTS;
    System.out.println("Latency results with No Delay over [" + MAX_ATTEMPTS + "] attempts.");
    System.out.println(
        " - Average Early Return response time with No Delay: "
            + averageearlyreturnruntime
            + " milliseconds");
    System.out.println(
        " - Average Workflow response time with No Delay: " + averagewfruntime + " milliseconds");
  }

  // Run workflow using 'updateWithStart'
  private static void runWorkflowWithUpdateWithStartMoreDelay(WorkflowClient client) {
    TransactionRequest txRequest =
        new TransactionRequest(
            "Bob", "Alice",
            1000); // Change this amount to a negative number to have initTransaction fail

    long totalwfruntime = 0;
    long totalearlyreturnruntime = 0;
    for (int i = 0; i < MAX_ATTEMPTS; i++) {
      TxResult updateResult = null;
      try {
        WorkflowOptions options = buildWorkflowOptionsMoreDelay();
        TransactionWorkflowMoreDelay workflow =
            client.newWorkflowStub(TransactionWorkflowMoreDelay.class, options);

        // System.out.println("Starting workflow with UpdateWithStart");

        UpdateWithStartWorkflowOperation<TxResult> updateOp =
            UpdateWithStartWorkflowOperation.newBuilder(workflow::returnInitResult)
                .setWaitForStage(WorkflowUpdateStage.COMPLETED) // Wait for update to complete
                .build();
        long starttime = System.currentTimeMillis();

        WorkflowUpdateHandle<TxResult> updateHandle =
            WorkflowClient.updateWithStart(workflow::processTransaction, txRequest, updateOp);

        updateResult = updateHandle.getResultAsync().get();
        long earlyreturnendtime = System.currentTimeMillis();
        totalearlyreturnruntime += earlyreturnendtime - starttime;

        // System.out.println(
        //     "Workflow initialized with result: "
        //         + updateResult.getStatus()
        //         + " (transactionId: "
        //         + updateResult.getTransactionId()
        //         + ")");

        TxResult result = WorkflowStub.fromTyped(workflow).getResult(TxResult.class);
        long wfendtime = System.currentTimeMillis();
        totalwfruntime += wfendtime - starttime;

        if (result == null) { // please stop
          System.out.println(
              "Workflow completed with result: "
                  + result.getStatus()
                  + ", update status: "
                  + updateResult.getStatus()
                  + " (transactionId: "
                  + result.getTransactionId()
                  + ")");
        }
      } catch (Exception e) {
        System.err.println("Transaction initialization failed: " + e.getMessage());
      }
    }

    long averagewfruntime = totalwfruntime / MAX_ATTEMPTS;
    long averageearlyreturnruntime = totalearlyreturnruntime / MAX_ATTEMPTS;
    System.out.println("Latency results with More Delay over [" + MAX_ATTEMPTS + "] attempts.");
    System.out.println(
        " - Average Early Return response time with More Delay: "
            + averageearlyreturnruntime
            + " milliseconds");
    System.out.println(
        " - Average Workflow response time with More Delay: " + averagewfruntime + " milliseconds");
  }

  // Build WorkflowOptions with task queue and unique ID
  private static WorkflowOptions buildWorkflowOptionsSomeDelay() {
    return WorkflowOptions.newBuilder()
        .setTaskQueue(TASK_QUEUE)
        .setWorkflowId(WORKFLOW_ID_PREFIX + "SomeDelay-" + System.currentTimeMillis())
        .build();
  }

  // Build WorkflowOptions with task queue and unique ID
  private static WorkflowOptions buildWorkflowOptionsNoDelay() {
    return WorkflowOptions.newBuilder()
        .setTaskQueue(TASK_QUEUE)
        .setWorkflowId(WORKFLOW_ID_PREFIX + "NoDelay-" + System.currentTimeMillis())
        .build();
  }

  // Build WorkflowOptions with task queue and unique ID
  private static WorkflowOptions buildWorkflowOptionsMoreDelay() {
    return WorkflowOptions.newBuilder()
        .setTaskQueue(TASK_QUEUE)
        .setWorkflowId(WORKFLOW_ID_PREFIX + "MoreDelay-" + System.currentTimeMillis())
        .build();
  }
}
