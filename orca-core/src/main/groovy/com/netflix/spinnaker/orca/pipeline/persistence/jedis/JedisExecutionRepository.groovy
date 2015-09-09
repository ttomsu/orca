package com.netflix.spinnaker.orca.pipeline.persistence.jedis

import java.time.Duration
import java.time.temporal.TemporalAmount
import java.util.function.Function
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.jackson.OrcaObjectMapper
import com.netflix.spinnaker.orca.pipeline.model.*
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionNotFoundException
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisCommands
import redis.clients.util.Pool
import rx.Observable
import rx.Scheduler
import rx.functions.Func1
import rx.schedulers.Schedulers
import static com.netflix.spinnaker.orca.ExecutionStatus.RUNNING
import static java.util.concurrent.Executors.newFixedThreadPool
import static rx.Observable.*

@Component
@Slf4j
@CompileStatic
class JedisExecutionRepository implements ExecutionRepository {

  private final Pool<Jedis> jedisPool
  private final ObjectMapper mapper = new OrcaObjectMapper()
  private final int chunkSize
  private final Scheduler queryAllScheduler
  private final Scheduler queryByAppScheduler

  @Autowired
  JedisExecutionRepository(
    Pool<Jedis> jedisPool,
    @Value('${threadPool.executionRepository:150}') int threadPoolSize,
    @Value('${chunkSize.executionRepository:75}') int threadPoolChunkSize
  ) {
    this(
      jedisPool,
      Schedulers.from(newFixedThreadPool(10)),
      Schedulers.from(newFixedThreadPool(threadPoolSize)),
      threadPoolChunkSize
    )
  }

  JedisExecutionRepository(
    Pool<Jedis> jedisPool,
    Scheduler queryAllScheduler,
    Scheduler queryByAppScheduler,
    int threadPoolChunkSize
  ) {
    this.jedisPool = jedisPool
    this.queryAllScheduler = queryAllScheduler
    this.queryByAppScheduler = queryByAppScheduler
    this.chunkSize = threadPoolChunkSize
  }

  @Override
  void store(Orchestration orchestration) {
    withJedis { Jedis jedis ->
      storeExecutionInternal(jedis, orchestration)
    }
  }

  @Override
  void store(Pipeline pipeline) {
    withJedis { Jedis jedis ->
      storeExecutionInternal(jedis, pipeline)
      jedis.sadd(pipelinesByAppKey(pipeline.application), pipeline.pipelineConfigId ?: "")
      jedis.zadd(executionsByPipelineKey(pipeline.pipelineConfigId), pipeline.buildTime, pipeline.id)
      if (pipeline.status == RUNNING) {
        jedis.sadd(runningPipelinesKey(pipeline.application), pipeline.id)
      } else {
        jedis.srem(runningPipelinesKey(pipeline.application), pipeline.id)
      }
    }
  }

  @Override
  void storeStage(PipelineStage stage) {
    withJedis { Jedis jedis ->
      storeStageInternal(jedis, Pipeline, stage)
    }
  }

  @Override
  void storeStage(Stage stage) {
    if (stage instanceof OrchestrationStage) {
      storeStage((OrchestrationStage) stage)
    } else {
      storeStage((PipelineStage) stage)
    }
  }

  @Override
  void storeStage(OrchestrationStage stage) {
    withJedis { Jedis jedis ->
      storeStageInternal(jedis, Orchestration, stage)
    }
  }

  @Override
  Pipeline retrievePipeline(String id) {
    withJedis { Jedis jedis ->
      retrieveInternal(jedis, Pipeline, id)
    }
  }

  @Override
  void deletePipeline(String id) {
    withJedis { Jedis jedis ->
      deleteInternal(jedis, Pipeline, id)
    }
  }

  @Override
  Observable<Pipeline> retrievePipelines() {
    all(Pipeline)
  }

  @Override
  Observable<Pipeline> retrievePipelinesForApplication(String application) {
    allForApplication(Pipeline, application)
  }

  @Override
  Observable<Pipeline> queryPipelines(String application, Set<ExecutionStatus> filter, Optional<Integer> maxPerType) {
    just(pipelinesByAppKey(application))
      .flatMap({ String key ->
      def pipelineConfigIds = withJedis { Jedis jedis ->
        jedis.smembers(key)
      }
      def pipelineStreams = pipelineConfigIds.collect {
        queryPipelinesByConfigId(it, filter, maxPerType)
      }
      def runningPipelinesStream = allRunningPipelinesFor(application)
      def stream = merge(pipelineStreams)
      merge(stream, runningPipelinesStream).distinct { Pipeline pipeline ->
        pipeline.id
      }
               } as Func1<String, Observable<Pipeline>>)
  }

  private Observable<Pipeline> queryPipelinesByConfigId(String pipelineConfigId, Set<ExecutionStatus> filter, Optional<Integer> max) {
    Observable<Pipeline> stream = just(pipelineConfigId)
      .flatMapIterable({ String _pipelineConfigId ->
      withJedis { Jedis jedis ->
        jedis.zrevrange(executionsByPipelineKey(_pipelineConfigId), 0, Long.MAX_VALUE)
      }
                       } as Func1<String, Iterable<String>>)
      .buffer(max.orElse(5))
      .flatMap({ Iterable<String> ids ->
      from(ids)
        .flatMap({ String executionId ->
        withJedis { Jedis jedis ->
          try {
            just(retrieveInternal(jedis, Pipeline, executionId))
          } catch (ExecutionNotFoundException e) {
            empty()
          }
        }
                 } as Func1<String, Observable<Pipeline>>)
        .filter({ Pipeline pipeline ->
        filter.isEmpty() || pipeline.status in filter
                } as Func1<Pipeline, Boolean>)
               } as Func1<Iterable<String>, Observable<Pipeline>>)

    return max.isPresent() ? stream.take(max.get()) : stream
  }

  private Observable<Pipeline> allRunningPipelinesFor(String application) {
    just(application)
      .flatMapIterable({
      withJedis { Jedis jedis ->
        jedis.smembers(runningPipelinesKey(application))
      }
    })
      .flatMap({ String executionId ->
      withJedis { Jedis jedis ->
        try {
          just(retrieveInternal(jedis, Pipeline, executionId))
        } catch (ExecutionNotFoundException e) {
          empty()
        }
      }
               } as Func1<String, Observable<Pipeline>>)
  }

  @Override
  Orchestration retrieveOrchestration(String id) {
    withJedis { Jedis jedis ->
      retrieveInternal(jedis, Orchestration, id)
    }
  }

  @Override
  void deleteOrchestration(String id) {
    withJedis { Jedis jedis ->
      deleteInternal(jedis, Orchestration, id)
    }
  }

  @Override
  Observable<Orchestration> retrieveOrchestrations() {
    all(Orchestration)
  }

  @Override
  Observable<Orchestration> retrieveOrchestrationsForApplication(String application) {
    allForApplication(Orchestration, application)
  }

  private void storeExecutionInternal(JedisCommands jedis, Execution execution) {
    def prefix = execution.getClass().simpleName.toLowerCase()

    if (!execution.id) {
      execution.id = UUID.randomUUID().toString()
      jedis.sadd(allExecutionsKey(execution.getClass()), execution.id)
      jedis.sadd(executionsByAppKey(execution.getClass(), execution.application), execution.id)
    }
    def json = mapper.writeValueAsString(execution)

    def key = "${prefix}:$execution.id"
    jedis.hset(key, "config", json)
  }

  private <T extends Execution> void storeStageInternal(Jedis jedis, Class<T> type, Stage<T> stage) {
    def json = mapper.writeValueAsString(stage)
    def key = "${type.simpleName.toLowerCase()}:stage:${stage.id}"
    jedis.hset(key, "config", json)
  }

  @CompileDynamic
  private <T extends Execution> T retrieveInternal(Jedis jedis, Class<T> type, String id) throws ExecutionNotFoundException {
    def key = "${type.simpleName.toLowerCase()}:$id"
    if (jedis.exists(key)) {
      def json = jedis.hget(key, "config")
      def execution = mapper.readValue(json, type)
      // PATCH to handle https://jira.netflix.com/browse/SPIN-784
      def originalStageCount = execution.stages.size()
      execution.stages = execution.stages.unique({ it.id })
      if (execution.stages.size() != originalStageCount) {
        log.warn(
          "Pipeline ${id} has duplicate stages (original count: ${originalStageCount}, unique count: ${execution.stages.size()})")
      }
      return sortStages(jedis, execution, type)
    } else {
      throw new ExecutionNotFoundException("No ${type.simpleName} found for $id")
    }
  }

  @CompileDynamic
  private <T extends Execution> T sortStages(Jedis jedis, T execution, Class<T> type) {
    List<Stage<T>> reorderedStages = []

    def childStagesByParentStageId = execution.stages.findAll { it.parentStageId != null }.groupBy { it.parentStageId }
    execution.stages.findAll { it.parentStageId == null }.each { Stage<T> parentStage ->
      reorderedStages << parentStage

      def children = childStagesByParentStageId[parentStage.id] ?: []
      while (!children.isEmpty()) {
        def child = children.remove(0)
        children.addAll(0, childStagesByParentStageId[child.id] ?: [])
        reorderedStages << child
      }
    }

    List<Stage<T>> retrievedStages = retrieveStages(jedis, type, reorderedStages.collect { it.id })
    def retrievedStagesById = retrievedStages.findAll { it?.id }.groupBy { it.id } as Map<String, Stage>
    execution.stages = reorderedStages.collect {
      def explicitStage = retrievedStagesById[it.id] ? retrievedStagesById[it.id][0] : it
      explicitStage.execution = execution
      return explicitStage
    }
    return execution
  }

  private <T extends Execution> List<Stage<T>> retrieveStages(Jedis jedis, Class<T> type, List<String> ids) {
    def pipeline = jedis.pipelined()
    ids.each { id ->
      pipeline.hget("${type.simpleName.toLowerCase()}:stage:${id}", "config")
    }
    def results = pipeline.syncAndReturnAll()
    return results.collect { it ? mapper.readValue(it as String, Stage) : null }
  }

  private <T extends Execution> void deleteInternal(Jedis jedis, Class<T> type, String id) {
    def key = "${type.simpleName.toLowerCase()}:$id"
    try {
      T item = retrieveInternal(jedis, type, id)
      jedis.srem(executionsByAppKey(type, item.application), id)
      if (item instanceof Pipeline) {
        ((Pipeline) item).with {
          jedis.zrem(executionsByPipelineKey(pipelineConfigId))
        }
      }

      item.stages.each { Stage stage ->
        def stageKey = "${type.simpleName.toLowerCase()}:stage:${stage.id}"
        jedis.hdel(stageKey, "config")
      }
    } catch (ExecutionNotFoundException ignored) {
      // do nothing
    } finally {
      jedis.hdel(key, "config")
      jedis.srem(allExecutionsKey(type), id)
    }
  }

  private <T extends Execution> Observable<T> all(Class<T> type) {
    retrieveObservable(type, allExecutionsKey(type), queryAllScheduler)
  }

  private <T extends Execution> Observable<T> allForApplication(Class<T> type, String application) {
    retrieveObservable(type, executionsByAppKey(type, application), queryByAppScheduler)
  }

  private <T extends Execution> Observable<T> retrieveObservable(Class<T> type, String lookupKey, Scheduler scheduler, TemporalAmount maxAge = Duration.ofDays(
    14)) {
    just(lookupKey)
      .flatMapIterable({ String key ->
      withJedis { Jedis jedis ->
        jedis.smembers(lookupKey)
      }
                       } as Func1<String, Set<String>>)
      .buffer(chunkSize)
      .flatMap { Collection<String> ids ->
      from(ids)
        .flatMap { String executionId ->
        withJedis { Jedis jedis ->
          try {
            return just(retrieveInternal(jedis, type, executionId))
          } catch (ExecutionNotFoundException ignored) {
            log.info("Execution (${executionId}) does not exist")
            jedis.srem(lookupKey, executionId)
          } catch (Exception e) {
            log.error("Failed to retrieve execution '${executionId}', message: ${e.message}", e)
          }
          return empty()
        }
      }
        .subscribeOn(scheduler)
    }
  }

  private String allExecutionsKey(Class<? extends Execution> type) {
    "allJobs:${type.simpleName.toLowerCase()}"
  }

  private String executionsByAppKey(Class<? extends Execution> type, String app) {
    "${type.simpleName.toLowerCase()}:app:${app}"
  }

  private String runningPipelinesKey(String application) {
    "pipelines:$application:running"
  }

  private String executionsByPipelineKey(String pipelineConfigId) {
    "pipeline:executions:$pipelineConfigId"
  }

  private String pipelinesByAppKey(String application) {
    "pipelineConfigs:$application"
  }

  private <T> T withJedis(Function<Jedis, T> action) {
    jedisPool.resource.withCloseable(action.&apply)
  }
}
