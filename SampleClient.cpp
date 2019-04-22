#include "MapReduceFramework.h"
#include <cstdio>
#include <string>
#include <array>
#include <unistd.h>
#include "SampleClient.h"




int main(int argc, char** argv)
{
	CounterClient client;
	InputVec inputVec;
	OutputVec outputVec;
	VString s1("abc");
	VString s2("abc");
	VString s3("abc");
	// VString s1("This string is full of characters");
	// VString s2("Multithreading is awesome");
	// VString s3("race conditions are bad");
	inputVec.push_back({nullptr, &s1});
	inputVec.push_back({nullptr, &s2});
	inputVec.push_back({nullptr, &s3});
	JobState state;
    JobState last_state={UNDEFINED_STAGE,0};
	JobHandle job = startMapReduceJob(client, inputVec, outputVec, 1);
	getJobState(job, &state);
    
	while (state.stage != REDUCE_STAGE || state.percentage != 100.0)
	{
        if (last_state.stage != state.stage || last_state.percentage != state.percentage){
            printf("stage %d, %f%% \n", 
			state.stage, state.percentage);
        }
		usleep(100000);
        last_state = state;
		getJobState(job, &state);
	}
	printf("stage %d, %f%% \n", 
			state.stage, state.percentage);
	printf("Done!\n");
	
	closeJobHandle(job);
	
	for (OutputPair& pair: outputVec) {
		char c = ((const KChar*)pair.first)->c;
		int count = ((const VCount*)pair.second)->count;
		printf("The character %c appeared %d time%s\n", 
			c, count, count > 1 ? "s" : "");
		delete pair.first;
		delete pair.second;
	}
	
	return 0;
}

