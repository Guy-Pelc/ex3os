#include <iostream>
#include "MapReduceFramework.h"
#include "SampleClient.h"
#include <algorithm>
#include <deque>

using namespace std;

// move this into object. now only supports 1 job//

JobState jobState;
IntermediateVec intermediateVec;
unsigned int mapCounter;
vector<IntermediateVec> sortedIntermediateVecs;

IntermediateVec curVec = {};

OutputVec _outputVec;

void printStatus()
{
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
JobHandle startMapReduceJob(const MapReduceClient& client,
	 const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel)
{
	cout<<"startMapReduceJob"<<endl;
	//init job object, currenly only 1 possible job, stored as global var.
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


	printStatus();
	}	

	cout<<"entering sorting phase"<<endl;
	sort(intermediateVec.begin(),intermediateVec.end(),[](IntermediatePair p1,IntermediatePair p2)
		{return *(p1.first)<*(p2.first);});
	printStatus();

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
	printStatus();

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
	printStatus();

	// while(true);
	return nullptr;
}

void waitForJob(JobHandle job)
{
	cout<<"waitForJob"<<endl;
}

//currently supports only one job, jobHandle does nothing.
void getJobState(JobHandle job, JobState* state)
{
	cout<<"getJobState"<<endl;
	*state = jobState;
	return;

}

void closeJobHandle(JobHandle job)
{
	cout<<"closeJobHandle"<<endl;
}
	
