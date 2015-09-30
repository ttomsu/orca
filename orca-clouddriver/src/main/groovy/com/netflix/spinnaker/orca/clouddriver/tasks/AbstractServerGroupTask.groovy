/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.clouddriver.tasks
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.orca.DefaultTaskResult
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.TaskResult
import com.netflix.spinnaker.orca.clouddriver.KatoService
import com.netflix.spinnaker.orca.clouddriver.model.TaskId
import com.netflix.spinnaker.orca.kato.pipeline.support.TargetServerGroup
import com.netflix.spinnaker.orca.kato.pipeline.support.TargetServerGroupResolver
import com.netflix.spinnaker.orca.pipeline.model.Stage
import org.springframework.beans.factory.annotation.Autowired
/**
 * @author sthadeshwar
 */
abstract class AbstractServerGroupTask extends AbstractCloudProviderAwareTask {

  @Autowired
  KatoService kato

  @Autowired
  ObjectMapper mapper

  abstract String getServerGroupAction()

  @Override
  TaskResult execute(Stage stage) {
    String cloudProvider = getCloudProvider(stage)
    Map operation = convert(stage)
    TaskId taskId = kato.requestOperations(cloudProvider, [[(serverGroupAction): operation]]).toBlocking().first()
    new DefaultTaskResult(
      ExecutionStatus.SUCCEEDED, [
      "notification.type"                                     : serverGroupAction.toLowerCase(),
      "kato.last.task.id"                                     : taskId,
      "deploy.account.name"                                   : operation.credentials,
      "serverGroupName"                                       : operation.serverGroupName,
      "asgName"                                               : operation.serverGroupName,  // TODO: Retire asgName
      ("targetop.asg.${serverGroupAction}.name".toString())   : operation.serverGroupName,
      ("targetop.asg.${serverGroupAction}.regions".toString()): operation.regions,
      "deploy.server.groups"                                  : (operation.regions as Collection<String>).collectEntries { [ (it): [operation.serverGroupName]] }
      ]
    )
  }

  Map convert(Stage stage) {
    def context = new HashMap(stage.context)
    context.serverGroupName = (context.serverGroupName ?: context.asgName) as String

    if (TargetServerGroup.isDynamicallyBound(stage)) {
      def tsg = TargetServerGroupResolver.fromPreviousStage(stage).find {
        (context.regions && context.regions.contains(it.location)) || (context.zones && context.zones.contains(it.location))
      }
      context.asgName = tsg.cluster
      context.serverGroupName = tsg.cluster

      if (context.zones && context.zones.contains(tsg.location)) {
        context.zone = tsg.location
        context.remove("zones")
      }
    }

    context
  }
}
