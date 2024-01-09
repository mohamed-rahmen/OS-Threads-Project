#include <pthread.h>
#include <atomic>
#include <set>
#include <queue>
#include "Barrier.h"
#include "MapReduceFramework.h"


typedef struct JobContext job;

typedef std::vector<IntermediatePair> pairsVec;

/**
 * a struct decribing the thread context that a thread belong to
 */
struct ThreadContext
{
    ThreadContext(int id, JobContext* jobContext)
            : ID(id), _doneJob(false),
              _threadMutex(PTHREAD_MUTEX_INITIALIZER), _jobContext(jobContext),
              _IntermediatePairs(std::vector<IntermediatePair>())
    {}


    /**
     * Destructor of the Thread context
     */
    ~ThreadContext()
    {
        pthread_mutex_destroy(&_threadMutex);
    }


    int ID;
    bool _doneJob;
    pthread_mutex_t _threadMutex;
    job* _jobContext;
    pairsVec _IntermediatePairs;
    std::pair<K1*, V1*> _pair{};
};
/**
 * Struct decribing the thread and its properties
 */
struct Thread
{
    Thread(int id, JobContext* jobContext)
            : _threadContext(ThreadContext(id, jobContext))
    {}


    pthread_t _thread{};
    ThreadContext _threadContext;
};

/**
 * A job context struct where it contains all the shared details of the job
 * the threads do.
 */
struct JobContext
{
    JobContext(int threadsNum, const MapReduceClient& client,
               const InputVec& inputVec, OutputVec& outputVec)
            : _jobState(new JobState), _multiThreadLevel(threadsNum), _client(client),
              _inputVec(inputVec), _outputVec(outputVec), _outputMutex(PTHREAD_MUTEX_INITIALIZER),
              _switchStateMutex(PTHREAD_MUTEX_INITIALIZER), _mapCount(0), _shuffleCount(0),
              _reduceCount(0), _pairsCount(0), _mapDoneCount(0), _reduceDoneCount(0),
              _barrier(Barrier(threadsNum))
    {
        _jobState->stage = UNDEFINED_STAGE;
        _jobState->percentage = 0.0;

        for (int i = 0; i < _multiThreadLevel; ++i)
        {
            _threads.push_back(new Thread(i, this));
        }
    }

    /**
     * JobContext destructor
     */
    ~JobContext()
    {
        delete _jobState;
        pthread_mutex_destroy(&_outputMutex);
        for (auto& _thread : _threads)
        {
            delete _thread;
        }

    }

    /**
     * where we update the keys vectors after they are done shuffling in order
     * to use the reduce on them.
     */
    void updateKeys()
    {
        for (std::pair<K2* const, std::vector<V2*>>& v : _IntermediateMap)
        {
            _keysVec.push_back(v.first);
        }
    }


    JobState* _jobState{};

    int _multiThreadLevel;

    const MapReduceClient& _client;
    const InputVec& _inputVec;
    OutputVec& _outputVec;

    pthread_mutex_t _outputMutex;
    pthread_mutex_t _switchStateMutex;

    std::atomic<int> _mapCount; //atomic counter for the map elements
    std::atomic<int> _shuffleCount; //atomic counter for the shuffle elements
    std::atomic<int> _reduceCount; //atomic counter for the reduce elements
    std::atomic<int> _pairsCount; //atomic counter for element in the IntermediateMap
    std::atomic<int> _mapDoneCount; //atomic counter for element after finishing map stag
    std::atomic<int> _reduceDoneCount; //atomic counter for element after finishing reduce stage

    std::vector<Thread*> _threads{};
    std::vector<K2*> _keysVec{}; // a vector that has key that are produced after finishing
                                 // the shuffling stage in order to help when using the reduce
                                 // function.
    IntermediateMap _IntermediateMap{};

    Barrier _barrier;
};