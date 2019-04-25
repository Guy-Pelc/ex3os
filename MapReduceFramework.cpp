#include <iostream>
#include "MapReduceFramework.h"
#include "SampleClient.h"
#include <algorithm>
#include <pthread.h>

using namespace std;

// move this into object. now only supports 1 job//
typedef struct {
	IntermediateVec intermediateVec;
	vector<IntermediateVec> sortedIntermediateVecs;
	IntermediateVec curVec = {};
} ThreadData;

//global

//must have different context per job.
class JobContext {
public:
	JobContext(	const MapReduceClient& client,
				const InputVec& inputVec, 
				OutputVec& outputVec,
				int multiThreadLevel) : 
				client(client),
				inputVec(inputVec),
				outputVec(outputVec) {
		thread_data_arr = new ThreadData[multiThreadLevel];
		threads_arr = new pthread_t[multiThreadLevel];
	}

	~JobContext(){
		delete[] thread_data_arr;
	}

	const MapReduceClient& client;
	const InputVec& inputVec; 
	OutputVec& outputVec;
	JobState jobState;


	unsigned int mapCounter;
	ThreadData* thread_data_arr;	
	pthread_t* threads_arr;
};




// per thread
IntermediateVec intermediateVec;
vector<IntermediateVec> sortedIntermediateVecs;
IntermediateVec curVec = {};

OutputVec _outputVec;

void printStatus(void* _context)
{
	JobContext* context = (JobContext *)_context; 
	JobState jobState = context->jobState;
	cout<<"printStatus:"<<endl;
	cout<<"jobState:"<<jobState.stage<<","<<jobState.percentage<<"%"<<endl;
	cout<<"intermediateVec:";
	for (IntermediatePair pair : intermediateVec)
	{
		cout<<"["<<static_cast<KChar*>(pair.first)->c<<":"<<static_cast<VCount*>(pair.second)->count<<"], ";
	}
	cout<<endl;
	cout<<"sortedIntermediateVecs"<<endl;
	

	for (IntermediateVec v : sortedIntermediateVecs)
	{
		for (IntermediatePair pair : v)
		{
			// cout<<"hello"<<endl;
			cout<<"["<<static_cast<KChar*>(pair.first)->c<<":"<<static_cast<VCount*>(pair.second)->count<<"], ";
		}
		cout<<endl;
	}
	cout<<"outputvec:"<<endl;
	for (OutputPair pair : _outputVec)
	{
		cout<<"["<<static_cast<KChar*>(pair.first)->c<<":"<<static_cast<VCount*>(pair.second)->count<<"], ";
	}

	cout<<"end print"<<endl;

}

void emit2 (K2* key, V2* value, void* context)
{
	cout<<"emit2"<<endl;
	intermediateVec.push_back({key,value});
}
void emit3 (K3* key, V3* value, void* context)
{
	cout<<"emit3"<<endl;
	_outputVec.push_back({key,value});
}

bool sortComp(IntermediatePair p1, IntermediatePair p2)
{	
	return *(p1.first)<*(p2.first);
}

bool isEqual(K2* i, K2* j)
{
	cout<<"comparing i,j = "<<static_cast<KChar*>(i)->c<<","<<static_cast<KChar*>(j)->c<<endl;
	cout<<"result "<<((!(*i<*j)) && (!(*j<*i)))<<endl;
	// while(1); 	
	return ((!(*i<*j)) && (!(*j<*i)));
}

void *doMap(void* _context)
{
	
}
void *doJob(void* _context)
{
	JobContext* context = (JobContext*)_context;
	JobState& jobState = context->jobState; 
	unsigned int& mapCounter = context->mapCounter; 
	
	const MapReduceClient& client = context->client;
	const InputVec& inputVec = context->inputVec;
	OutputVec& outputVec = context->outputVec;



	jobState = {MAP_STAGE,0};

	//start mapping phase:
	mapCounter = 0;
	unsigned int inputSize = inputVec.size();
	while(mapCounter < inputSize)
	{
	
	// inputVec.erase(inputVec.begin());
	InputPair firstPair = inputVec[mapCounter];
	client.map(firstPair.first,firstPair.second,nullptr);
	mapCounter++;
	jobState.percentage =100* (double(mapCounter) / double(inputSize));


	printStatus(_context);
	}	

	cout<<"entering sorting phase"<<endl;
	sort(intermediateVec.begin(),intermediateVec.end(),[](IntermediatePair p1,IntermediatePair p2)
		{return *(p1.first)<*(p2.first);});
	printStatus(_context);

	cout<<"entering shuffle and reduce phase"<<endl;
	jobState = {REDUCE_STAGE,0};

	K2* curKeyp = intermediateVec.back().first;
	cout<<"curkey is:"<<static_cast<KChar*>(curKeyp)->c<<endl;

	while(!intermediateVec.empty())
	{
		if (!isEqual(curKeyp,intermediateVec.back().first))
		{
			sortedIntermediateVecs.push_back(curVec);
			curVec.clear();
			curKeyp = intermediateVec.back().first;
		}

		curVec.push_back(intermediateVec.back());
		intermediateVec.pop_back();
	}

	sortedIntermediateVecs.push_back(curVec);
	cout<<"curvecsize= "<< curVec.size()<<endl;
	cout<<"sortedIntermediateVecs size: "<<sortedIntermediateVecs.size()<<endl;
	printStatus(_context);

	cout<<"entering reduce phase"<<endl;

	_outputVec = outputVec; 
	// IntermediateVec nextVec;

	// client.reduce(&sortedIntermediateVecs.back(),nullptr);
	// sortedIntermediateVecs.pop_back();
	// cout<<"here"<<endl;
	// client.reduce(&sortedIntermediateVecs.back(),nullptr);
	
	while(!sortedIntermediateVecs.empty())
	{
		// nextVec = sortedIntermediateVecs.back();
		client.reduce(&sortedIntermediateVecs.back(),nullptr);
		sortedIntermediateVecs.pop_back();
	}

	jobState = {REDUCE_STAGE,100};
	cout<<"reduce phase done"<<endl;
	printStatus(_context);

	pthread_exit(nullptr);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
	 const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel)
{
	JobContext *context = new JobContext(client,inputVec,outputVec,multiThreadLevel);
	cout<<"startMapReduceJob"<<endl;
	//init job object, currenly only 1 possible job, stored as global var.
	
	pthread_create(&(context->threads_arr[0]),nullptr, &doJob,(void*)context);
	

	// while(true);
	return (void*)context;
}

void waitForJob(JobHandle job)
{
	cout<<"waitForJob"<<endl;
}

//currently supports only one job, jobHandle does nothing.
void getJobState(JobHandle job, JobState* state)
{
	cout<<"getJobState"<<endl;
	JobContext* context = (JobContext *)job; 
	*state = context->jobState;
	return;

}

void closeJobHandle(JobHandle job)
{
	cout<<"closeJobHandle"<<endl;
	JobContext* jc = (JobContext* )job;
	delete jc;
	return;
}
	
