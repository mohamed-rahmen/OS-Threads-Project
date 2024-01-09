#include <iostream>
#include "MapReduceFramework.h"
#include "JobContext.h"

#define JOB_CONTEXT_CAST(arg) JobContext *jc = (JobContext *)arg
#define THREAD_CONTEXT_CAST(arg) ThreadContext *tc = (ThreadContext *)arg
#define ERROR_HANDLER(statement, errorMsg) if (statement != 0) {errorExit(errorMsg);}
#define CREATION_FAILER "system error: Fail to create thread"
#define JOIN_FAILER "system error: Fail to join thread"
#define MUTEX_FAILER "system error: Fail to lock or unlock thread"

void* reduceThread(void* arg);

/**
 * ethis function is called when stumbling upon an error and it exits the program
 * showing an error message
 * @param errorMsg: error message we show
 */
void errorExit(const std::string& errorMsg)
{
    std::cerr << errorMsg << std::endl;
    exit(1);
}


/**
 * This function creates (K2*, V2*)pair
 * @param key : key that allows us to compare the elements and sort them
 * @param value : value that represents the value of each key
 * @param context that the pair belongs to
 */
void emit2(K2* key, V2* value, void* context)
{
    THREAD_CONTEXT_CAST(context);
    ERROR_HANDLER(pthread_mutex_lock(&tc->_threadMutex), MUTEX_FAILER)
    tc->_IntermediatePairs.emplace_back(key, value);
    tc->_jobContext->_pairsCount++;
    ERROR_HANDLER(pthread_mutex_unlock(&tc->_threadMutex), MUTEX_FAILER)
}


/**
 * This function creates (K3*, V3*)pair
 * @param key that allows us to compare the elements and sort them
 * @param value V3 value that represents the value of each key
 * @param context that the pair belongs to
 */
void emit3(K3* key, V3* value, void* context)
{
    THREAD_CONTEXT_CAST(context);
    ERROR_HANDLER(pthread_mutex_lock(&tc->_jobContext->_outputMutex), MUTEX_FAILER)
    tc->_jobContext->_outputVec.emplace_back(key, value);
    ERROR_HANDLER(pthread_mutex_unlock(&tc->_jobContext->_outputMutex), MUTEX_FAILER)
}


/**
 * mapping function that we apply on each element in order to produce the Intermediate elements
 * @param arg: The Thread context
 * @return a null pointer
 */
void* mapThread(void* arg)
{
    THREAD_CONTEXT_CAST(arg);
    JobContext* jc = tc->_jobContext;

    int idx = jc->_mapCount++;
    while (idx < (int) jc->_inputVec.size())
    {
        tc->_pair = jc->_inputVec[idx];
        tc->_jobContext->_client.map(tc->_pair.first, tc->_pair.second, arg);
        jc->_mapDoneCount++;
        idx = jc->_mapCount++;
    }
    jc->_barrier.barrier();
    reduceThread(arg);
    return nullptr;
}


/**
 * This phase reads the Map phase output and combines them into a single IntermediateMap
 * where we basically sort the elements of the Intermediate pairs
 * @param arg: The Thread context
 * @return a nullpointer and we update our IntermediateMap
 */
void* shuffleThread(void* arg)
{
    JOB_CONTEXT_CAST(arg);
    int x = 0;
    while (true)
    {

        if (x == 1)
        {
            break;
        }
        for (int i = 0; i < (int) jc->_threads.size() - 1; ++i)
        {
            if (jc->_jobState->stage == SHUFFLE_STAGE && jc->_shuffleCount == jc->_pairsCount)
            {
                x = 1;
                break;
            }
            if (jc->_mapDoneCount == (int)jc->_inputVec.size() && jc->_pairsCount != 0)
            {
                ERROR_HANDLER(pthread_mutex_lock(&jc->_switchStateMutex), MUTEX_FAILER)
                jc->_jobState->stage = SHUFFLE_STAGE;
                ERROR_HANDLER(pthread_mutex_unlock(&jc->_switchStateMutex), MUTEX_FAILER)
            }
            Thread* currentThread = jc->_threads[i];
            ERROR_HANDLER(pthread_mutex_lock(&currentThread->_threadContext._threadMutex),
                          MUTEX_FAILER)

            for (auto pair : currentThread->_threadContext._IntermediatePairs)
            {
                jc->_IntermediateMap[pair.first].push_back(pair.second);
                jc->_shuffleCount++;
            }

            currentThread->_threadContext._IntermediatePairs.clear();
            ERROR_HANDLER(pthread_mutex_unlock(&currentThread->_threadContext._threadMutex),
                          MUTEX_FAILER)
        }
    }
    jc->updateKeys();
    jc->_jobState->stage = REDUCE_STAGE;
    jc->_barrier.barrier();
    reduceThread(&jc->_threads[jc->_multiThreadLevel - 1]->_threadContext);
    return nullptr;
}


/**
 * this function is applied to each of the sorted sequences of
   intermediary elements that are in the IntermediateMap, producing a sequence of output elements.
 * @param arg: The Thread context
 * @return a nullpointer and we update our reduce map
 */
void* reduceThread(void* arg)
{
    THREAD_CONTEXT_CAST(arg);
    JobContext* jc = tc->_jobContext;

    int idx = jc->_reduceCount++;
    while (idx < (int) jc->_keysVec.size())
    {
        K2* key = jc->_keysVec[idx];
        jc->_client.reduce(key, jc->_IntermediateMap[key], arg);
        jc->_reduceDoneCount++;
        idx = jc->_reduceCount++;
    }
    tc->_doneJob = true;
    return nullptr;
}


/**
 * this function creates the threads according to the number we want to be created
 * @param jc the job context the threads would belong to
 * @param threadsNum the number of threads we want to create
 * @param f pointer to the function we want the threads to do
 */
void createThreads(JobContext* jc, int threadsNum, void* (* f)(void*))
{
    for (int i = 0; i < threadsNum; ++i)
    {
        ERROR_HANDLER(pthread_create(&jc->_threads[i]->_thread, nullptr, f,
                                     &jc->_threads[i]->_threadContext), CREATION_FAILER)
    }
}


/**
 * An identifier of a running job. Returned when starting a job and used by other framework
 * functions.
 * @param client: The implementation of MapReduceClient class, in other words the task that the
                    framework should run.
 * @param inputVec the vector that has the input elements that we want to do their job
 * @param outputVec the output result would be stored in this vector
 * @param multiThreadLevel  the number of threads we to create
 * @return a job handle object
 */
JobHandle
startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec,
                  int multiThreadLevel)
{
    auto* jobHandle = new JobContext(multiThreadLevel, client, inputVec, outputVec);
    jobHandle->_jobState->stage = MAP_STAGE;
    createThreads(jobHandle, multiThreadLevel - 1, mapThread);
    ERROR_HANDLER(pthread_create(&jobHandle->_threads[multiThreadLevel - 1]->_thread,
                                 nullptr, shuffleThread, jobHandle), CREATION_FAILER)
    return jobHandle;
}


/**
 * This function receives a job handle and wait until it is finished
 * @param job the job object it is applied on
 */
void waitForJob(JobHandle job)
{
    JOB_CONTEXT_CAST(job);
    for (int i = 0; i < jc->_multiThreadLevel; ++i)
    {
        int res = pthread_join(jc->_threads[i]->_thread, nullptr);
        if (res != 0 && res != ESRCH)
        {
            std::cerr << JOIN_FAILER << std::endl;
            exit(1);
        }
    }
}


/**
 * The job state is updated here, where both the percentage and the state of the job
 * is updated after checking the number of elements that are present in our maps(finished mapping
 * or sorting)
 * @param job: The job context
 * @param state: the state of the current job
 */
void getJobState(JobHandle job, JobState* state)
{
    JOB_CONTEXT_CAST(job);
    ERROR_HANDLER(pthread_mutex_lock(&jc->_switchStateMutex), MUTEX_FAILER)

    if (jc->_jobState->stage == MAP_STAGE)
    {
        jc->_jobState->percentage = (float) jc->_mapDoneCount.load() / jc->_inputVec.size() * 100;
    }

    if (jc->_jobState->stage == SHUFFLE_STAGE)
    {
        jc->_jobState->percentage = (float) jc->_shuffleCount.load() / jc->_pairsCount * 100;
    }

    if (jc->_jobState->stage == REDUCE_STAGE)
    {
        jc->_jobState->percentage = (float) jc->_reduceDoneCount.load() / jc->_keysVec.size() * 100;
    }

    state->stage = jc->_jobState->stage;
    state->percentage = jc->_jobState->percentage;
    ERROR_HANDLER(pthread_mutex_unlock(&jc->_switchStateMutex), MUTEX_FAILER)
}


/**
 * this function deletes all the threads and the classes that are related to the job
 * after the job is done
 * @param job we finished
 */
void closeJobHandle(JobHandle job)
{
    JOB_CONTEXT_CAST(job);
    waitForJob(job);
    delete jc;
}
