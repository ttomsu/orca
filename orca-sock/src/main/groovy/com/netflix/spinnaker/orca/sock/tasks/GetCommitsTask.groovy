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

package com.netflix.spinnaker.orca.sock.tasks

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.orca.DefaultTaskResult
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.RetryableTask
import com.netflix.spinnaker.orca.TaskResult
import com.netflix.spinnaker.orca.front50.Front50Service
import com.netflix.spinnaker.orca.front50.model.Application
import com.netflix.spinnaker.orca.oort.OortService
import com.netflix.spinnaker.orca.pipeline.model.Stage
import com.netflix.spinnaker.orca.sock.SockService
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import retrofit.RetrofitError

import java.util.concurrent.TimeUnit

@Slf4j
@Component
class GetCommitsTask implements RetryableTask {

  long backoffPeriod = 1000
  long timeout = TimeUnit.MINUTES.toMillis(2)

  @Autowired
  OortService oortService

  @Autowired
  ObjectMapper objectMapper

  @Autowired(required = false)
  SockService sockService

  @Autowired
  Front50Service front50Service


  @Override
  TaskResult execute(Stage stage) {
    if (!sockService) {
      return DefaultTaskResult.SUCCEEDED
    }

    def globalAccount = front50Service.credentials.find { it.global }
    def applicationAccount = globalAccount?.name ?: stage.context.account
    Application application = front50Service.get(applicationAccount, stage.context.application)

    String repoType = application.repoType
    String projectKey = application.repoProjectKey
    String repositorySlug = application.repoSlug
    String region = stage.context?.source?.region ?: stage.context?.availabilityZones?.findResult { key, value -> key }
    String account = stage.context?.source?.account ?: stage.context?.account
    String toCommit
    String fromCommit

    String ancestorAmi
    String targetAmi
    def targetRegion

    TypeReference<List> jsonListType = new TypeReference<List>() {}
    TypeReference<Map> jsonMapType = new TypeReference<Map>() {}

    // figure out if we are diff'ing two asgs in the same/separate cluster
    if(stage.context.canary?.canaryDeployments) { // separate cluster diff
      targetRegion = stage.context."kato.tasks"[0].resultObjects[0].region
      ancestorAmi = stage.context.canary.canaryDeployments?.baselineCluster?.imageId
      targetAmi = stage.context.canary.canaryDeployments?.canaryCluster?.imageId

    } else if (stage.context.get("kato.tasks")) { // assume same cluster asg diff
      String ancestorAsg = stage.context.get("kato.tasks")?.find { item ->
        item.find { key, value ->
          key == 'resultObjects'
        }
      }?.resultObjects?.ancestorServerGroupNameByRegion?.find {
        it.find { key, value ->
          key == region
        }
      }?.get(region)
      String sourceCluster
      if (ancestorAsg.lastIndexOf("-") > 0) {
        sourceCluster = ancestorAsg.substring(0, ancestorAsg.lastIndexOf("-"))
      } else {
        sourceCluster = ancestorAsg
      }
      Map sourceServerGroup = objectMapper.readValue(oortService.getServerGroup(stage.context.application,
        account, sourceCluster,
        ancestorAsg, region, "aws").body.in(), jsonMapType)

      stage.context."deploy.server.groups".each {
        targetRegion = it.key
      }

      // deploy task sets this one
      targetAmi = stage.context.deploymentDetails.find { it.region == targetRegion }?.ami

      // copyLastAsg sets this one
      if (!targetAmi) {
        targetAmi = stage.context.amiName
      }
      ancestorAmi = sourceServerGroup.launchConfig.imageId
    }

    if (projectKey && repositorySlug && repoType && targetRegion && account && ancestorAmi && targetAmi) {
      try {



        def sourceAmiDetails = objectMapper.readValue(oortService.getByAmiId("aws", account,
          targetRegion, ancestorAmi).body.in(), jsonListType)

        String sourceAppVersion = sourceAmiDetails[0]?.tags?.appversion
        if(sourceAppVersion) {
          toCommit = sourceAppVersion.substring(0, sourceAppVersion.indexOf('/')).substring(sourceAppVersion.lastIndexOf('.') + 1)
        } else {
          return new DefaultTaskResult(ExecutionStatus.SUCCEEDED)
        }

        def targetAmiDetails = objectMapper.readValue(oortService.getByAmiId("aws", account,
          targetRegion, targetAmi).body.in(), jsonListType)

        String targetAppVersion = targetAmiDetails[0]?.tags?.appversion
        if(targetAppVersion) {
          fromCommit = targetAppVersion.substring(0, targetAppVersion.indexOf('/')).substring(targetAppVersion.lastIndexOf('.') + 1)
        } else {
          return new DefaultTaskResult(ExecutionStatus.SUCCEEDED)
        }

        if(toCommit && fromCommit) {
          List commits = sockService.compareCommits(repoType, projectKey, repositorySlug, [to: toCommit, from: fromCommit, limit: 100])
          def commitsList = []
          commits.each {
            // add commits to the task output
            commitsList << [displayId: it.displayId, id: it.id, authorDisplayName: it.authorDisplayName,
                            timestamp: it.timestamp, commitUrl: it.commitUrl, message: it.message]
          }
          return new DefaultTaskResult(ExecutionStatus.SUCCEEDED, [commits: [(targetRegion): commitsList]])
        } else {
          // dont fail if we cant determine source and target commits
          return new DefaultTaskResult(ExecutionStatus.SUCCEEDED, [commits: [(targetRegion): []]])
        }
      } catch (RetrofitError e) {
        if ([503, 500, 404].contains(e.response?.status)) {
          log.warn("Http ${e.response.status} received from `sock` (${repoType}, ${projectKey}, ${repositorySlug}, ${toCommit}, ${fromCommit}) , retrying...")
          return new DefaultTaskResult(ExecutionStatus.RUNNING)
        }
        throw e
      }
    } else { // skip if we don't have the repo information
      return new DefaultTaskResult(ExecutionStatus.SUCCEEDED)
    }
  }
}
