package com.netflix.spinnaker.orca.pipeline.persistence

import com.netflix.spinnaker.kork.jedis.EmbeddedRedis
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.pipeline.model.DefaultTask
import com.netflix.spinnaker.orca.pipeline.model.Pipeline
import com.netflix.spinnaker.orca.pipeline.model.PipelineStage
import com.netflix.spinnaker.orca.pipeline.persistence.jedis.JedisExecutionRepository
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.util.Pool
import spock.lang.*
import static com.netflix.spinnaker.orca.ExecutionStatus.*
import static java.util.Collections.emptySet
import static java.util.UUID.randomUUID
import static org.hamcrest.Matchers.containsInAnyOrder
import static spock.util.matcher.HamcrestSupport.expect

class ExecutionQuerySpec extends Specification {

  @Shared @AutoCleanup("destroy") EmbeddedRedis embeddedRedis

  def setupSpec() {
    embeddedRedis = EmbeddedRedis.embed()
  }

  def cleanup() {
    embeddedRedis.jedis.flushDB()
  }

  Pool<Jedis> jedisPool = new JedisPool("localhost", embeddedRedis.@port)
  @AutoCleanup def jedis = jedisPool.resource
  @Subject def repository = new JedisExecutionRepository(jedisPool, 1, 50)

  def "brings back only executions for the specified app"() {
    given: "pipelines for different apps"
    pipelineApps.each {
      def pipeline = Pipeline
        .builder()
        .withApplication(it)
        .withPipelineConfigId(randomUUID().toString())
        .build()
      repository.store(pipeline)
    }

    when: "we retrieve an unfiltered list of executions"
    def executions = repository
      .queryPipelines(app, emptySet(), Optional.empty())
      .toBlocking()
      .toIterable()
      .toList()

    then: "all the results belong to the requested app"
    executions.application.every {
      it == app
    }

    where:
    app = "cantaloupe"
    pipelineApps = [app, "honeydew"]
  }

  def "brings back no more than #n executions per pipeline"() {
    given: "n+1 executions for different pipelines"
    (1..pipelines).each {
      def pipelineConfigId = randomUUID().toString()
      (n + 1).times {
        def pipeline = Pipeline
          .builder()
          .withApplication(app)
          .withPipelineConfigId(pipelineConfigId)
          .build()
        repository.store(pipeline)
      }
    }

    when: "we retrieve an unfiltered list of n executions"
    def executions = repository
      .queryPipelines(app, emptySet(), Optional.of(n))
      .toBlocking()
      .toIterable()
      .toList()

    then: "we get back pipelines * n executions"
    executions.size() == pipelines * n

    and: "we get n of each pipeline"
    def grouped = executions.groupBy { it.pipelineConfigId }
    grouped.size() == pipelines
    grouped.values().every { it.size() == n }

    where:
    app = "cantaloupe"
    n = 2
    pipelines = 2
  }

  def "executions are returned newest first"() {
    given:
    n.times {
      def pipeline = Pipeline
        .builder()
        .withApplication(app)
        .withPipelineConfigId(pipelineConfigId)
        .withBuildTime(randomizer.nextLong())
        .build()
      repository.store(pipeline)
    }

    when:
    def executions = repository
      .queryPipelines(app, emptySet(), Optional.empty())
      .toBlocking()
      .toIterable()
      .toList()

    then:
    executions.buildTime == executions.buildTime.sort().reverse()

    where:
    app = "cantaloupe"
    n = 10
    pipelineConfigId = randomUUID().toString()
    randomizer = new Random()
  }

  @Unroll
  def "brings back only executions that match the filter #filter"() {
    given:
    ExecutionStatus.values().each { status ->
      def pipeline = Pipeline
        .builder()
        .withApplication(app)
        .withPipelineConfigId(pipelineConfigId)
        .withStage("foo")
        .build()
      ((PipelineStage) pipeline.stages[0]).tasks = [new DefaultTask()]
      pipeline.stages[0].status = status
      if (status == CANCELED) {
        pipeline.canceled = true
      }
      repository.store(pipeline)
    }

    when:
    def executions = repository
      .queryPipelines(app, filter, Optional.empty())
      .toBlocking()
      .toIterable()
      .toList()

    then:
    executions.status.unique().size() == filter.size()
    executions.status.every { it in filter }

    where:
    filter                         | _
    EnumSet.of(RUNNING, SUCCEEDED) | _
    EnumSet.of(RUNNING)            | _

    app = "cantaloupe"
    pipelineConfigId = randomUUID().toString()
  }

  def "returns running executions even if filtered out"() {
    given:
    [FAILED, RUNNING].each { status ->
      def pipeline = Pipeline
        .builder()
        .withApplication(app)
        .withPipelineConfigId(pipelineConfigId)
        .withStage("foo")
        .build()
      ((PipelineStage) pipeline.stages[0]).tasks = [new DefaultTask()]
      pipeline.stages[0].status = status
      repository.store(pipeline)
    }

    when:
    def executions = repository
      .queryPipelines(app, filter, Optional.empty())
      .toBlocking()
      .toIterable()
      .toList()

    then:
    executions.size() == 2
    expect executions.status, containsInAnyOrder(RUNNING, FAILED)

    where:
    filter = EnumSet.of(FAILED)
    app = "cantaloupe"
    pipelineConfigId = randomUUID().toString()
  }

  def "brings back all running executions regardless of requested maximum"() {
    given:
    (n + 1).times {
      def pipeline = Pipeline
        .builder()
        .withApplication(app)
        .withPipelineConfigId(pipelineConfigId)
        .withStage("foo")
        .build()
      ((PipelineStage) pipeline.stages[0]).tasks = [new DefaultTask()]
      pipeline.stages[0].status = RUNNING
      repository.store(pipeline)
    }

    when:
    def executions = repository
      .queryPipelines(app, emptySet(), Optional.of(n))
      .toBlocking()
      .toIterable()
      .toList()

    then:
    executions.size() == n + 1

    and:
    executions.id == executions.id.unique()

    where:
    app = "cantaloupe"
    pipelineConfigId = randomUUID().toString()
    n = 2
  }

  def "does not load more pipelines into memory than required"() {
    given:
    (n + 1).times {
      def pipeline = Pipeline
        .builder()
        .withApplication(app)
        .withPipelineConfigId(pipelineConfigId)
        .withStage("foo")
        .build()
      repository.store(pipeline)
    }

    when:
    def executions = repository
      .queryPipelines(app, emptySet(), Optional.of(n))
      .toBlocking()
      .toIterable()
      .toList()

    then:
    1 == 1

    where:
    app = "cantaloupe"
    pipelineConfigId = randomUUID().toString()
    n = 2
  }
}
