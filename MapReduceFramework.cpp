#include <iostream>
#include "MapReduceFramework.h"
#include "SampleClient.h"
#include <algorithm>
#include <pthread.h>
#include <atomic>
#include "Barrier.h"

using namespace std;



class ThreadData {
	
public:
	IntermediateVec intermediateVec;
	
	const MapReduceClient& client;
	const InputVec& inputVec; 
	OutputVec& outputVec;
	JobState& jobState;
	const int tid;
	atomic<unsigned int>& mapCounter; 
	vector<IntermediateVec>& sortedIntermediateVecs;
	ThreadData** thread_data_parr;
	const int multiThreadLevel;

	pthread_mutex_t* mutexp;
	pthread_cond_t* cvp;
	pthread_mutex_t* mutexReducep;
	Barrier* barrierp;
	int& totalIPairs;
	int& reducedIPairs;

	ThreadData(	const MapReduceClient& client,
			   	const InputVec& inputVec, 
			  	OutputVec& outputVec,
			  	JobState& jobState, 
			  	const int tid,
			  	atomic<unsigned int>& mapCounter,
			  	vector<IntermediateVec>& sortedIntermediateVecs,
			  	ThreadData** thread_data_parr,
			  	const int multiThreadLevel,
			  	pthread_mutex_t* mutexp,
			  	pthread_cond_t* cvp,
			  	pthread_mutex_t* mutexReducep,
			  	Barrier* barrierp,
			  	int& totalIPairs,
			  	int& reducedIPairs) : 
				client(client),
				inputVec(inputVec),
				outputVec(outputVec),
				jobState(jobState),
				tid(tid),
				mapCounter(mapCounter),
				sortedIntermediateVecs(sortedIntermediateVecs),
				thread_data_parr(thread_data_parr),
				multiThreadLevel(multiThreadLevel),
				mutexp(mutexp),
				cvp(cvp),
				mutexReducep(mutexReducep),
				barrierp(barrierp),
				totalIPairs(totalIPairs),
				reducedIPairs(reducedIPairs)
				{
					cout<<"ThreadData constructor "<<tid<<endl;
				}


};


//global

//must have different context per job.
class JobContext {
public:

	const MapReduceClient& client;
	const InputVec& inputVec; 
	OutputVec& outputVec;
	JobState jobState;
	const int multiThreadLevel;
	vector<IntermediateVec> sortedIntermediateVecs;
	atomic<unsigned int> mapCounter;
	ThreadData** thread_data_parr;	
	pthread_t* threads_arr;

	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t mutexReduce = PTHREAD_MUTEX_INITIALIZER;
	pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
	Barrier* barrier;
	int totalIPairs=0;
	int reducedIPairs = 0;

	JobContext(	const MapReduceClient& client,
				const InputVec& inputVec, 
				OutputVec& outputVec,
				const int multiThreadLevel) : 
				client(client),
				inputVec(inputVec),
				outputVec(outputVec),
				multiThreadLevel(multiThreadLevel),
				mapCounter(0) {
		cout<<"jobContext constructor"<<endl;
		barrier = new Barrier(multiThreadLevel);
		thread_data_parr = new ThreadData*[multiThreadLevel];
		for (int i=0;i<multiThreadLevel;++i){
			thread_data_parr[i] = new ThreadData(	client, 
													inputVec, 
													outputVec, 
													jobState, 
													i, 
													mapCounter,
													sortedIntermediateVecs, 
													thread_data_parr,
													multiThreadLevel,
													&mutex,
													&cv,
													&mutexReduce,
													barrier,
													totalIPairs,
													reducedIPairs);
		}
		threads_arr = new pthread_t[multiThreadLevel];		
	}
	
	
	~JobContext(){
		for (int i=0;i<multiThreadLevel;++i)
		{
			delete thread_data_parr[i];
		}
		delete[] thread_data_parr;
	}

};


void printStatus(void* _context)
{
	ThreadData* context = (ThreadData *)_context; 
	JobState& jobState = context->jobState;

	cout<<"printStatus: Thread "<<context->tid<<endl;
	cout<<"jobState:"<<jobState.stage<<","<<jobState.percentage<<"%"<<endl;
	cout<<"intermediateVec:";
	IntermediateVec intermediateVec;
	for (int i=0;i<context->multiThreadLevel;++i){
		cout<<"thread:"<<i<<endl;
		 intermediateVec = context->thread_data_parr[i]->intermediateVec;
		for (IntermediatePair pair : intermediateVec){
			cout<<"["<<static_cast<KChar*>(pair.first)->c<<":"<<static_cast<VCount*>(pair.second)->count<<"], ";
			}
		cout<<endl;
	}

	cout<<endl;
	cout<<"sortedIntermediateVecs"<<endl;
	

	for (IntermediateVec v : context->sortedIntermediateVecs){
		for (IntermediatePair pair : v){
			// cout<<"hello"<<endl;
			cout<<"["<<static_cast<KChar*>(pair.first)->c<<":"<<static_cast<VCount*>(pair.second)->count<<"], ";
		}
		cout<<endl;
	}


	cout<<"outputvec:"<<endl;
	for (OutputPair pair : context->outputVec){
		cout<<"["<<static_cast<KChar*>(pair.first)->c<<":"<<static_cast<VCount*>(pair.second)->count<<"], ";
	}

	cout<<"end print"<<endl;

}

void emit2 (K2* key, V2* value, void* context){
	// cout<<"emit2"<<endl;
	ThreadData* _context = (ThreadData*) context;
	_context->intermediateVec.push_back({key,value});


	cout<<"intermediateVec["<<_context->tid<<"] added "<< static_cast<KChar*>(key)->c<<endl;
}

void emit3 (K3* key, V3* value, void* context){
	ThreadData* _context = (ThreadData*) context;
	pthread_mutex_t* mutexReducep = _context->mutexReducep;

	pthread_mutex_lock(mutexReducep);
		_context->outputVec.push_back({key,value});
	pthread_mutex_unlock(mutexReducep);
	// cout<<"end emit3"<<endl;
}

bool sortComp(IntermediatePair p1, IntermediatePair p2)
{	
	return *(p1.first)<*(p2.first);
}

bool isEqual(K2* i, K2* j)
{
	// cout<<"comparing i,j = "<<static_cast<KChar*>(i)->c<<","<<static_cast<KChar*>(j)->c<<endl;
	// cout<<"result "<<((!(*i<*j)) && (!(*j<*i)))<<endl;
	// while(1); 	
	return ((!(*i<*j)) && (!(*j<*i)));
}

void doMap(void* _context)
{
	cout<<"doMap"<<endl;
	ThreadData* context = (ThreadData *)_context;
	JobState& jobState = context->jobState; 
	pthread_mutex_t* mutexp = context->mutexp;
	atomic<unsigned int>& mapCounter = context->mapCounter; 
	
	const MapReduceClient& client = context->client;
	const InputVec& inputVec = context->inputVec;
	

	unsigned int inputSize = inputVec.size();
	InputPair firstPair;

	while(1){
		unsigned int oldVal = mapCounter++;
		if (oldVal < inputSize){
			firstPair = inputVec[oldVal];
			client.map(firstPair.first,firstPair.second,_context);
			pthread_mutex_lock(mutexp);
				jobState.percentage += 100/(float)inputVec.size();
			pthread_mutex_unlock(mutexp);
		}
		else {break;}
	}

	return;
}

/* returns max key or nullptr if all iVecs are empty
*/
K2* getMaxKey(void* _context){
	ThreadData* context = (ThreadData*)_context;
	K2* maxKeyp = nullptr; //context->thread_data_parr[0]->intermediateVec.back().first; 
	K2* compKeyp;
	IntermediateVec iVec;
	for (int i=0;i<context->multiThreadLevel;++i){
		iVec = context->thread_data_parr[i]->intermediateVec;
		if (!iVec.empty()){
			compKeyp = context->thread_data_parr[i]->intermediateVec.back().first;	
			// cout<<"compkey is:"<<static_cast<KChar*>(compKeyp)->c<<endl;
			if (maxKeyp == nullptr) {
				maxKeyp = compKeyp; 
				//cout<<"max changed nullptr;"<<endl;
			}
			else if (*maxKeyp<*compKeyp) {
				maxKeyp=compKeyp; 
				//cout<<"max changed <;"<<endl;
			}	
		}
	}
	// if (maxKeyp == nullptr){cout<<"maxkey is nullptr"<<endl;}
	// else {cout<<"maxkey is: "<<static_cast<KChar*>(maxKeyp)->c<<endl;}
	return maxKeyp;
}

void doShuffle(void* _context){
	// cout<<endl<<"entering doShuffle"<<endl;

	ThreadData* context = (ThreadData*)_context;
	JobState& jobState = context->jobState; 
	vector<IntermediateVec>& sortedIntermediateVecs = context->sortedIntermediateVecs;

	pthread_mutex_t* mutexp =  context->mutexp;
	pthread_cond_t* cvp = context->cvp;

	int& totalIPairs = context->totalIPairs;
	//get totalIPairs
	totalIPairs = 0;
for (int i=0;i<context->multiThreadLevel;++i)
{
	totalIPairs += context->thread_data_parr[i]->intermediateVec.size();
}
// cout<<"totalIPairs = "<<totalIPairs<<endl;

	jobState = {REDUCE_STAGE,0};

	K2* curKeyp = getMaxKey(_context);
	IntermediateVec curSortedVec;
	while (!(curKeyp==nullptr)){
		for (int i=0;i<context->multiThreadLevel;++i){
			IntermediateVec &intermediateVec =  context->thread_data_parr[i]->intermediateVec;
			while( !intermediateVec.empty()){
				// cout<<"i_size:"<<intermediateVec.size()<<endl;
				if (isEqual(intermediateVec.back().first, curKeyp)){
					curSortedVec.push_back(intermediateVec.back());
					intermediateVec.pop_back();
					continue;
				}
				break;
			}
		}
		pthread_mutex_lock(mutexp);
			sortedIntermediateVecs.push_back(curSortedVec);
			cout<<"produced sortedVec: "<<"{";
			//print sortedvec:
			for (int i=0;i<curSortedVec.size();++i){
				cout<<"("<<	static_cast<KChar*>(curSortedVec[i].first)->c<<","<<
							static_cast<VCount*>(curSortedVec[i].second)->count<<") ";
			}
			cout<<"}"<<endl;
			pthread_cond_broadcast(cvp);
		pthread_mutex_unlock(mutexp);
		curSortedVec.clear();
		curKeyp = getMaxKey(_context);
		// cout<<"curkey is:"<<static_cast<KChar*>(curKeyp)->c<<endl;
	}
	
	cout<<"shuffling done"<<endl;

	//get maxkey
}

void doReduce(void* _context){
	// cout<<"doReduce"<<endl;
	ThreadData* context = (ThreadData*)_context;
	vector<IntermediateVec>& sortedIntermediateVecs = context->sortedIntermediateVecs; 
	const MapReduceClient& client = context->client;
	JobState& jobState = context->jobState; 

	int& totalIPairs = context->totalIPairs;
	int& reducedIPairs = context->reducedIPairs;

	pthread_mutex_t* mutexp =  context->mutexp;
	pthread_cond_t* cvp = context->cvp;
	
	int curVecSize;
	while(1){
		pthread_mutex_lock(mutexp);
			while(sortedIntermediateVecs.empty()){
				pthread_cond_wait(cvp,mutexp);
			}
			curVecSize = sortedIntermediateVecs.back().size();
			cout<<"curVecSize: "<<curVecSize<<endl;
			cout<<"reducing key: "<<static_cast<KChar*>(sortedIntermediateVecs.back().back().first)->c<<endl;
			client.reduce(&sortedIntermediateVecs.back(),_context);
			sortedIntermediateVecs.pop_back();
			reducedIPairs += curVecSize;
			jobState.percentage = 100*reducedIPairs/float(totalIPairs);
		pthread_mutex_unlock(mutexp);
	}
}

void *doJob(void* _context)
{
	cout<<"doJob"<<endl;

	ThreadData* context = (ThreadData*)_context;
	JobState& jobState = context->jobState; 
	
	IntermediateVec& intermediateVec= context->intermediateVec;

	Barrier* barrierp = context->barrierp;

	if (context->tid == 0){cout<<"entering map phase"<<endl;}
	context->jobState = {MAP_STAGE,0};

	doMap(_context);	
	
	if (context->tid==0){cout<<"entering sort phase"<<endl;}
	
	sort(intermediateVec.begin(),intermediateVec.end(),[](IntermediatePair p1,IntermediatePair p2)
		{return *(p1.first)<*(p2.first);});


	barrierp->barrier();
	cout<<"thread "<<context->tid<<" out of barrier"<<endl;


	if (context->tid ==0){

		jobState = {REDUCE_STAGE,0};
		cout<<"entering shuffle and reduce phase"<<endl;
		printStatus(_context);

		doShuffle(_context);
	}		

	doReduce(_context);



	pthread_exit(nullptr);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel)
{
	cout<<"startMapReduceJob"<<endl;
	JobContext *context = new JobContext(client,inputVec,outputVec,multiThreadLevel);
	

	printf("stage %d, %f%% \n", 
		context->jobState.stage, context->jobState.percentage);

	for (int i=0;i<multiThreadLevel;++i){
		pthread_create(&(context->threads_arr[i]),nullptr, &doJob,(void*)context->thread_data_parr[i]);
	}
	
	return (void*)context;
}

void waitForJob(JobHandle job)
{
	cout<<"waitForJob"<<endl;
}

//currently supports only one job, jobHandle does nothing.
void getJobState(JobHandle job, JobState* state)
{
		// cout<<"getJobState"<<endl;
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

void test2()
{
	// VCount vc = {1};
	// KChar c = {'c'};
	// KChar b = {'b'};
	// KChar a = {'a'};
	// KChar d = {'d'};
	// KChar e = {'e'};
	// KChar f = {'f'};
	// KChar h = {'h'};
	// KChar g = {'g'};
	// KChar i = {'i'};

	// IntermediatePair ip1 = {&a,&vc};
	// IntermediatePair ip2 = {&i,&vc};

	// K2* k1p = ip1.first;
	// K2* k2p = ip2.first;
	

	// K2* k3p = &b;
	// K2* k4p = &d;

	// bool res = *k3p<*k4p;
	// bool res2 = b<*k3p;
	// cout<<res<<endl;
}
void test(){

	cout<<"hi from test"<<endl;

	CounterClient client;
	InputVec inputVec;
	OutputVec  outputVec;
	int multiThreadLevel=4;

	JobContext *context = new JobContext(client,inputVec,outputVec,multiThreadLevel);
	
	
	VCount vc = {1};
	KChar c = {'c'};
	KChar b = {'b'};
	KChar a = {'a'};
	KChar d = {'d'};
	KChar e = {'e'};
	KChar f = {'f'};
	KChar h = {'h'};
	KChar g = {'g'};
	KChar i = {'i'};

	IntermediateVec& intermediateVec = context->thread_data_parr[0]->intermediateVec;
	intermediateVec.push_back({&a,&vc});
	intermediateVec.push_back({&b,&vc});
	intermediateVec.push_back({&c,&vc});
	intermediateVec.push_back({&d,&vc});
	intermediateVec.push_back({&e,&vc});
	intermediateVec.push_back({&f,&vc});

	

	IntermediateVec& intermediateVec2 = context->thread_data_parr[1]->intermediateVec;
	// intermediateVec2.push_back({&a,&vc});
	// intermediateVec2.push_back({&b,&vc});
	// intermediateVec2.push_back({&c,&vc});
	intermediateVec2.push_back({&d,&vc});
	intermediateVec2.push_back({&e,&vc});
	intermediateVec2.push_back({&f,&vc});
	// intermediateVec2.push_back({&g,&vc});
	// intermediateVec2.push_back({&h,&vc});
	// intermediateVec2.push_back({&i,&vc});



	IntermediateVec& intermediateVec3 = context->thread_data_parr[2]->intermediateVec;
	intermediateVec3.push_back({&a,&vc});
	intermediateVec3.push_back({&b,&vc});
	intermediateVec3.push_back({&c,&vc});
	// intermediateVec3.push_back({&d,&vc});
	// intermediateVec3.push_back({&e,&vc});
	// intermediateVec3.push_back({&f,&vc});
	intermediateVec3.push_back({&g,&vc});
	intermediateVec3.push_back({&h,&vc});
	intermediateVec3.push_back({&i,&vc});


	IntermediateVec& intermediateVec4 = context->thread_data_parr[3]->intermediateVec;
	// intermediateVec4.push_back({&a,&vc});
	// intermediateVec4.push_back({&b,&vc});
	// intermediateVec4.push_back({&c,&vc});
	// intermediateVec4.push_back({&d,&vc});
	// intermediateVec4.push_back({&e,&vc});
	// intermediateVec4.push_back({&f,&vc});
	intermediateVec4.push_back({&g,&vc});
	intermediateVec4.push_back({&h,&vc});
	intermediateVec4.push_back({&i,&vc});

	doShuffle((void*)context->thread_data_parr[0]);
	
	return;
}