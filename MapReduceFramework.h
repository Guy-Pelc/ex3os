#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"

/**
* an identifier of a running job. Returned when starting a job
* and used by other framework functions (for example to get the state of a job).
*/
typedef void* JobHandle;

enum stage_t {UNDEFINED_STAGE=0, MAP_STAGE=1, REDUCE_STAGE=2};


/**
* a struct with quantizes the state of a job, inlcluding:
* - stage_t stage - an enum (0-undefine, 1-Map,2-reduce)
* - float percentage - job progress of current stage (i.e., the percentage of keys
*   that were processed out of all the keys that should be processed in the stage).
*/
typedef struct {
	stage_t stage;
	float percentage;
} JobState;

/**
* This function produces a (K2*,V2*) pair.
* The context can be used to get pointers into the framework's variables and data structures.
* Its exact type is implementation dependent.
*/
void emit2 (K2* key, V2* value, void* context);

/**
* This function produces a (K3*,V3*) pair.
* The context can be used to get pointers into the framework's variables and tata structures.
* Its exact type is implementation dependent.
*/
void emit3 (K3* key, V3* value, void* context);

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

//change input vec back to const!
JobHandle startMapReduceJob(const MapReduceClient& client,
	 const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel);

/**
* a function that gets the job handle returned by startMapReduceFramework
* and waits until it is finished
*/
void waitForJob(JobHandle job);

/**
* this function gets a job handle and inserts its current state 
* the a given JobState struct.
*/
void getJobState(JobHandle job, JobState* state);

/**
* Releasing all resources of a job. You should prevent releasing
* resources before the job is finished. After this function is called
* the job handle will be invalid.
*/
void closeJobHandle(JobHandle job);
	
	
#endif //MAPREDUCEFRAMEWORK_H
