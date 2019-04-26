#include <iostream>
#include "MapReduceFramework.h"
// #include "SampleClient.h" 	
#include <algorithm>
#include <pthread.h>
#include <atomic>
#include "Barrier.h"
#include <semaphore.h>

using namespace std;


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
	int totalIPairs=0;
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

		// cout<<"SharedContext constructor"<<endl;
		barrier = new Barrier(multiThreadLevel);
		intermediateVec_arr = new IntermediateVec[multiThreadLevel];
		
		threads_arr = new pthread_t[multiThreadLevel];

		sem_init(&sem,0,0);	
	}

	~SharedContext(){
		// cout<<"sharedContext destructor"<<endl;
		delete barrier;
		delete[] intermediateVec_arr;
		delete[] threads_arr;
	}
};

typedef struct {
	SharedContext* sharedContextp;
	int tid;
} ThreadContext;

class JobContext {
public:
	SharedContext* sharedContextp ;
	ThreadContext* threadContext_arr;

	JobContext(	const MapReduceClient& client,
				const InputVec& inputVec, 
				OutputVec& outputVec,
				const int multiThreadLevel) {
		sharedContextp = new SharedContext(client, inputVec,outputVec,multiThreadLevel);

		threadContext_arr = new ThreadContext[multiThreadLevel];
		for (int i=0;i<multiThreadLevel;++i){
			threadContext_arr[i] = {sharedContextp,i};
		}
	}
	~JobContext(){
		// cout<<"JobContext destructor"<<endl;
		delete threadContext_arr;
		delete sharedContextp;

	}
};

/*
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
*/

void emit2 (K2* key, V2* value, void* context){
	// cout<<"emit2"<<endl;
	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;
	int tid = tc->tid;
	sc->intermediateVec_arr[tid].push_back({key,value});


	// cout<<"intermediateVec_arr["<<tid<<"] added "<< static_cast<KChar*>(key)->c<<endl;
}

void emit3 (K3* key, V3* value, void* context){
	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;
	
	pthread_mutex_t* outMutexp = &(sc->outMutex);

	pthread_mutex_lock(outMutexp);
		sc->outputVec.push_back({key,value});
	pthread_mutex_unlock(outMutexp);
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

void doMap(void* context)
{
	// cout<<"doMap"<<endl;

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
			pthread_mutex_lock(jsMutexp);
				totalMapped += 1;
				jobState.percentage = 100*totalMapped/(float)inputVec.size();
			pthread_mutex_unlock(jsMutexp);
		}
		else {break;}
	}

	return;
}

/* returns max key or nullptr if all iVecs are empty
*/
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

void doShuffle(void* context){
	// cout<<endl<<"entering doShuffle"<<endl;

	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;
	
	JobState& jobState = sc->jobState; 
	vector<IntermediateVec>& sortedIntermediateVecs = sc->sortedIntermediateVecs;

	pthread_mutex_t* mutexp =  &(sc->mutex);
	sem_t* sem = &(sc->sem);

	int& totalIPairs = sc->totalIPairs;
	//get totalIPairs
	totalIPairs = 0;
for (int i=0;i<sc->multiThreadLevel;++i)
{
	totalIPairs += sc->intermediateVec_arr[i].size();
}
// cout<<"totalIPairs = "<<totalIPairs<<endl;

	jobState = {REDUCE_STAGE,0};

	K2* curKeyp = getMaxKey(context);
	IntermediateVec curSortedVec;
	while (!(curKeyp==nullptr)){
		for (int i=0;i<sc->multiThreadLevel;++i){
			IntermediateVec &intermediateVec =  sc->intermediateVec_arr[i];
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
		pthread_mutex_unlock(mutexp);
		// cout<<"posting to semaphore"<<endl;
		sem_post(sem);
		
		/*
		pthread_mutex_lock(mutexp);
			sortedIntermediateVecs.push_back(curSortedVec);
			// cout<<"produced sortedVec: "<<"{";
			// //print sortedvec:
			// for (unsigned int i=0;i<curSortedVec.size();++i){
			// 	cout<<"("<<	static_cast<KChar*>(curSortedVec[i].first)->c<<","<<
			// 				static_cast<VCount*>(curSortedVec[i].second)->count<<") ";
			// }
			// cout<<"}"<<endl;
			pthread_cond_broadcast(cvp);
		pthread_mutex_unlock(mutexp);
		*/
		curSortedVec.clear();
		curKeyp = getMaxKey(context);
		// cout<<"curkey is:"<<static_cast<KChar*>(curKeyp)->c<<endl;
		
	}
	
	// cout<<"shuffling done"<<endl;

	//get maxkey
}

void doReduce(void* context){
	// cout<<"doReduce"<<endl;
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
	sem_wait(sem);
	if (jobState.percentage == 100) {sem_post(sem); return;}


	pthread_mutex_lock(mutexp);
		curIVec = sortedIntermediateVecs.back();
		sortedIntermediateVecs.pop_back();
	pthread_mutex_unlock(mutexp);

	curVecSize = curIVec.size();
	client.reduce(&curIVec,context);

	pthread_mutex_lock(jsMutexp);
		reducedIPairs += curVecSize;
		jobState.percentage = 100*reducedIPairs/float(totalIPairs);
	pthread_mutex_unlock(jsMutexp);
		if (jobState.percentage == 100){sem_post(sem); return;}
	}

	


	/*
	while(1){

		pthread_mutex_lock(mutexp);
			while(sortedIntermediateVecs.empty()){
				pthread_cond_wait(cvp,mutexp);
			}
			curVecSize = sortedIntermediateVecs.back().size();
			// cout<<"curVecSize: "<<curVecSize<<endl;
			// cout<<"reducing key: "<<static_cast<KChar*>(sortedIntermediateVecs.back().back().first)->c<<endl;
			client.reduce(&sortedIntermediateVecs.back(),context);
			sortedIntermediateVecs.pop_back();
			reducedIPairs += curVecSize;
			jobState.percentage = 100*reducedIPairs/float(totalIPairs);
		pthread_mutex_unlock(mutexp);
	}
	*/
}

void *doJob(void* context)
{
	// cout<<"doJob"<<endl;

	ThreadContext* tc = (ThreadContext*) context;
	SharedContext* sc = tc-> sharedContextp;
	int tid = tc->tid;

	JobState& jobState = sc->jobState; 
	
	IntermediateVec& intermediateVec= sc->intermediateVec_arr[tid];

	Barrier* barrierp = sc->barrier;

	// if (tid == 0){cout<<"entering map phase"<<endl;}
	sc->jobState = {MAP_STAGE,0};

	doMap(context);	
	
	// if (tid==0){cout<<"entering sort phase"<<endl;}
	
	sort(intermediateVec.begin(),intermediateVec.end(),[](IntermediatePair p1,IntermediatePair p2)
		{return *(p1.first)<*(p2.first);});


	barrierp->barrier();
	// cout<<"thread "<<tid<<" out of barrier"<<endl;


	if (tid ==0){

		jobState = {REDUCE_STAGE,0};
		// cout<<"entering shuffle and reduce phase"<<endl;
		// printStatus(context);

		doShuffle(context);
	}		

	doReduce(context);

	// cout<<"pthread: "<<tid<<" terminating"<<endl;
	pthread_exit(nullptr);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel)
{
	// cout<<"startMapReduceJob"<<endl;
	JobContext *jobContext = new JobContext(client,inputVec,outputVec,multiThreadLevel);
	

	printf("stage %d, %f%% \n", 
		jobContext->sharedContextp->jobState.stage, jobContext->sharedContextp->jobState.percentage);

	for (int i=0;i<multiThreadLevel;++i){
		ThreadContext& tc = jobContext->threadContext_arr[i]; 
		pthread_create(&(jobContext->sharedContextp->threads_arr[i]),nullptr, &doJob,(void*)&tc);
	}
	
	return (void*)jobContext;
}

void waitForJob(JobHandle job)
{
	JobContext* jc = (JobContext*)job;
	SharedContext* sc = jc->sharedContextp;

	for (int i=0;i<sc->multiThreadLevel;++i)
	{
		// cout<<"waiting for thread "<<i<<endl;
		pthread_join(sc->threads_arr[i],nullptr);
	}
	// cout<<"all threads terminated. job done."<<endl;
	/*
	pthread_mutex_lock(mutexJobDonep);
		if(!jobDone){
			pthread_cond_wait(cvJobDonep,mutexJobDonep);
		}
	pthread_mutex_unlock(mutexJobDonep);
	*/
	// cout<<"done waitingForJob"<<endl;
	return;
}

//currently supports only one job, jobHandle does nothing.
void getJobState(JobHandle job, JobState* state)
{
		// cout<<"getJobState"<<endl;
	JobContext* jc= (JobContext *)job; 
	SharedContext* sc = jc->sharedContextp;
	pthread_mutex_lock(&(sc->jsMutex));
		*state = sc->jobState;
	pthread_mutex_unlock(&(sc->jsMutex));
	return;

}

void closeJobHandle(JobHandle job)
{
	waitForJob(job);
	// cout<<"closeJobHandle"<<endl;
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
/*
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
*/