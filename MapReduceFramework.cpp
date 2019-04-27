#include <iostream>
#include "MapReduceFramework.h"
#include <algorithm>
#include <pthread.h>
#include <atomic>
#include "Barrier.h"
#include <semaphore.h>
#include <stdlib.h>

using namespace std;

/*Class that holds all data shared between threads*/
class SharedContext{
public:
	const MapReduceClient& client;
	const InputVec& inputVec; 
	OutputVec& outputVec;

	JobState jobState;
	const int multiThreadLevel;

	vector<IntermediateVec> sortedIntermediateVecs;
	atomic<unsigned int> mapCounter;
	IntermediateVec* intermediateVec_arr;	
	pthread_t* threads_arr;

	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t outMutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_cond_t cv = PTHREAD_COND_INITIALIZER;

	Barrier* barrier;
	int totalIPairs = 0;
	int reducedIPairs = 0;
	int totalMapped = 0;

	pthread_mutex_t jsMutex = PTHREAD_MUTEX_INITIALIZER;
	sem_t sem;


	SharedContext(
				const MapReduceClient& client,
				const InputVec& inputVec, 
				OutputVec& outputVec,
				const int multiThreadLevel) : 

				client(client),
				inputVec(inputVec),
				outputVec(outputVec),
				multiThreadLevel(multiThreadLevel),
				mapCounter(0) {

		barrier = new Barrier(multiThreadLevel);
		if (!barrier) {cerr<<"error: mem alloc failed"; exit(1);}

		intermediateVec_arr = new IntermediateVec[multiThreadLevel];
		if (!intermediateVec_arr){cerr<<"error: mem alloc failed"; exit(1);}
		
		threads_arr = new pthread_t[multiThreadLevel];
		if (!threads_arr){cerr<<"error: mem alloc failed"; exit(1);}

		sem_init(&sem,0,0);	

		jobState = {UNDEFINED_STAGE,0};
	}

	~SharedContext(){
		delete barrier;
		delete[] intermediateVec_arr;
		delete[] threads_arr;
	}
};

/* struct that holds each thread's data */
typedef struct {
	SharedContext* sharedContextp;
	int tid;
} ThreadContext;

/* class that holds all of the job's data */
class JobContext {
public:
	SharedContext* sharedContextp ;
	ThreadContext* threadContext_arr;

	JobContext(	const MapReduceClient& client,
				const InputVec& inputVec, 
				OutputVec& outputVec,
				const int multiThreadLevel) {
		sharedContextp = new SharedContext(client, inputVec,outputVec,multiThreadLevel);
		if (!sharedContextp){cerr<<"error: mem alloc failed"; exit(1);}

		threadContext_arr = new ThreadContext[multiThreadLevel];
		if (!threadContext_arr){cerr<<"error: mem alloc failed"; exit(1);}

		for (int i=0;i<multiThreadLevel;++i){
			threadContext_arr[i] = {sharedContextp,i};
		}
	}
	~JobContext(){
		delete[] threadContext_arr;
		delete sharedContextp;
	}
};

/**
* This function produces a (K2*,V2*) pair.
* The context can be used to get pointers into the framework's variables and data structures.
* Its exact type is implementation dependent.
*/
void emit2 (K2* key, V2* value, void* context){
	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;
	int tid = tc->tid;
	sc->intermediateVec_arr[tid].push_back({key,value});
}

/**
* This function produces a (K3*,V3*) pair.
* The context can be used to get pointers into the framework's variables and tata structures.
* Its exact type is implementation dependent.
*/
void emit3 (K3* key, V3* value, void* context){
	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;
	
	pthread_mutex_t* outMutexp = &(sc->outMutex);

	if (pthread_mutex_lock(outMutexp)!=0) {cerr<<"error on pthread_mutex_lock"; exit(1);}
		sc->outputVec.push_back({key,value});
	if (pthread_mutex_unlock(outMutexp)!=0) {cerr<<"error on pthread_mutex_unlock"; exit(1);}
}

/* returns bool: key of p1 < key of p2 */
bool sortComp(IntermediatePair p1, IntermediatePair p2)
{	
	return *(p1.first)<*(p2.first);
}

bool isEqual(K2* i, K2* j)
{
	return ((!(*i<*j)) && (!(*j<*i)));
}

/* runs the mapping phase of the algorithm, possibly with multiple threads */
void doMap(void* context)
{
	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;

	JobState& jobState = sc->jobState; 
	pthread_mutex_t* jsMutexp = &(sc->jsMutex);
	atomic<unsigned int>& mapCounter = sc->mapCounter; 
	const MapReduceClient& client = sc->client;
	const InputVec& inputVec = sc->inputVec;
	int& totalMapped = sc->totalMapped;

	unsigned int inputSize = inputVec.size();
	InputPair firstPair;

	while(1){
		unsigned int oldVal = mapCounter++;
		if (oldVal < inputSize){
			firstPair = inputVec[oldVal];
			client.map(firstPair.first,firstPair.second,context);
			if (pthread_mutex_lock(jsMutexp)!=0){cerr<<"error on pthread_mutex_lock"; exit(1);}
				totalMapped += 1;
				jobState.percentage = 100*totalMapped/(float)inputVec.size();
			if (pthread_mutex_unlock(jsMutexp)!=0){cerr<<"error on pthread_mutex_unlock"; exit(1);}
		}
		else {break;}
	}
	return;
}


/* returns max key or nullptr if all iVecs are empty */
K2* getMaxKey(void* context){
	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;
	
	K2* maxKeyp = nullptr; 
	K2* compKeyp;
	IntermediateVec iVec;

	for (int i=0;i<sc->multiThreadLevel;++i){
		iVec = sc->intermediateVec_arr[i];
		if (!iVec.empty()){
			compKeyp = sc->intermediateVec_arr[i].back().first;	
			if (maxKeyp == nullptr) {
				maxKeyp = compKeyp; 
			}
			else if (*maxKeyp<*compKeyp) {
				maxKeyp=compKeyp; 
			}	
		}
	}
	return maxKeyp;
}

/* runs the shuffle phase of the algorithm, by one thread only */
void doShuffle(void* context){
	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;

	vector<IntermediateVec>& sortedIntermediateVecs = sc->sortedIntermediateVecs;
	pthread_mutex_t* mutexp =  &(sc->mutex);
	sem_t* sem = &(sc->sem);
	int& totalIPairs = sc->totalIPairs;

	totalIPairs = 0;
for (int i=0;i<sc->multiThreadLevel;++i)
{
	totalIPairs += sc->intermediateVec_arr[i].size();
}

	K2* curKeyp = getMaxKey(context);
	IntermediateVec curSortedVec;
	while (!(curKeyp==nullptr)){
		for (int i=0;i<sc->multiThreadLevel;++i){
			IntermediateVec &intermediateVec =  sc->intermediateVec_arr[i];
			while( !intermediateVec.empty()){
				if (isEqual(intermediateVec.back().first, curKeyp)){
					curSortedVec.push_back(intermediateVec.back());
					intermediateVec.pop_back();
					continue;
				}
				break;
			}
		}

		if (pthread_mutex_lock(mutexp)!=0){cerr<<"error on pthread_mutex_lock"; exit(1);}
		sortedIntermediateVecs.push_back(curSortedVec);
		if (pthread_mutex_unlock(mutexp)!=0){cerr<<"error on pthread_mutex_unlock"; exit(1);}
		
		if (sem_post(sem)!=0){cerr<<"error on sem_post";exit(1);}
		
		curSortedVec.clear();
		curKeyp = getMaxKey(context);
		
	}
}

/* runs the reduce phase of the algorithm, possibly by multiple threads*/
void doReduce(void* context){
	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;
	
	vector<IntermediateVec>& sortedIntermediateVecs = sc->sortedIntermediateVecs; 
	const MapReduceClient& client = sc->client;
	JobState& jobState = sc->jobState; 

	int& totalIPairs = sc->totalIPairs;
	int& reducedIPairs = sc->reducedIPairs;

	pthread_mutex_t* mutexp =  &(sc->mutex);
	sem_t* sem = &(sc->sem);
	int curVecSize;
	IntermediateVec curIVec;
	pthread_mutex_t* jsMutexp = &(sc->jsMutex);


	while(1){	
	if (sem_wait(sem)!=0){cerr<<"error on sem_wait";exit(1);}
	// if the job is done, release all remaining threads through the semaphore and return.
	if (jobState.percentage == 100) {
		if (sem_post(sem)!=0){cerr<<"error on sem_post";exit(1);}
		return;}


	if (pthread_mutex_lock(mutexp)!=0){cerr<<"error on pthread_mutex_lock"; exit(1);}
		curIVec = sortedIntermediateVecs.back();
		sortedIntermediateVecs.pop_back();
	if (pthread_mutex_unlock(mutexp)!=0){cerr<<"error on pthread_mutex_unlock"; exit(1);}

	curVecSize = curIVec.size();
	client.reduce(&curIVec,context);

	if (pthread_mutex_lock(jsMutexp)!=0){cerr<<"error on pthread_mutex_lock"; exit(1);}
		reducedIPairs += curVecSize;
		jobState.percentage = 100*reducedIPairs/float(totalIPairs);
	if (pthread_mutex_unlock(jsMutexp)!=0){cerr<<"error on pthread_mutex_unlock"; exit(1);}
	
	// if the job is done, release all remaining threads through the semaphore and return.
	if (jobState.percentage == 100){
		if (sem_post(sem)!=0){cerr<<"error on sem_post";exit(1);}
		return;}
	}
}

/* runs the mergeSort algorithm, entry point for all threads*/
void *doJob(void* context){
	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;
	
	int tid = tc->tid;
	JobState& jobState = sc->jobState; 
	IntermediateVec& intermediateVec= sc->intermediateVec_arr[tid];
	Barrier* barrierp = sc->barrier;

	//map stage
	sc->jobState = {MAP_STAGE,0};
	doMap(context);	
	
	//sort stage	
	sort(intermediateVec.begin(),intermediateVec.end(),[](IntermediatePair p1,IntermediatePair p2)
		{return *(p1.first)<*(p2.first);});


	barrierp->barrier();
	

	// shuffle stage (for one thread only)
	if (tid ==0){
		jobState = {REDUCE_STAGE,0};
		doShuffle(context);
	}
	// reduce stage		
	doReduce(context);


	pthread_exit(nullptr);
}

/**
* This function starts running the MapReduce algorithm (with several threads)
* and returns a JobHandle.
* client - the implementation of MapReduceClient or in other words the task that 
*          the framework should run.
* inputVec - a vector of type std::vector<std::pair<K1*,V1*>>, the input elements
* outputVec - a vector of type std::vector<std::pair<K3*,V3*>>, to which
*             the output elements will be added before returning.
*             You can assume that the outputVec is empty.
* multiThreadLevel - the number of worker threads to be used for running the algorithm.
*                    You can assume that this argument is valid (greater or equal to 1).
*/
JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel)
{
	JobContext *jobContext = new JobContext(client,inputVec,outputVec,multiThreadLevel);
	if (!jobContext) {cerr<<"error with mem alloc"; exit(1);}
	
	for (int i=0;i<multiThreadLevel;++i){
		ThreadContext& tc = jobContext->threadContext_arr[i]; 
		if (pthread_create(&(jobContext->sharedContextp->threads_arr[i]), nullptr, &doJob,(void*)&tc) != 0){
			cerr<<"error with pthread_create";exit(1);
		}
	}
	
	return (void*)jobContext;
}

/**
* a function that gets the job handle returned by startMapReduceFramework
* and waits until it is finished
*/
void waitForJob(JobHandle job)
{
	JobContext* jc = (JobContext*)job;
	SharedContext* sc = jc->sharedContextp;

	for (int i=0;i<sc->multiThreadLevel;++i)
	{
		if (pthread_join(sc->threads_arr[i],nullptr)!=0) {cerr<<"error with pthread_join";exit(1);}
	}
	return;
}

/**
* this function gets a job handle and inserts its current state 
* the a given JobState struct.
*/
void getJobState(JobHandle job, JobState* state)
{
	JobContext* jc= (JobContext *)job; 
	SharedContext* sc = jc->sharedContextp;
	pthread_mutex_t* jsMutexp = &(sc->jsMutex);

	if(pthread_mutex_lock(jsMutexp)!=0){cerr<<"error with pthread_mutex_lock"; exit(1);}
	*state = sc->jobState;
	if(pthread_mutex_unlock(jsMutexp)!=0){cerr<<"error with pthread_mutex_unlock"; exit(1);}
	
	return;
}

/**
* Releasing all resources of a job. This method will wait until the job is done 
*.After this function is called the job handle will be invalid.
*/
void closeJobHandle(JobHandle job)
{
	waitForJob(job);
	JobContext* jc = (JobContext* )job;
	delete jc;
	return;
}
