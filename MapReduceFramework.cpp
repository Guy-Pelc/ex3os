#include <iostream>
#include "MapReduceFramework.h"
#include "SampleClient.h"
#include <algorithm>
#include <pthread.h>

using namespace std;


// move this into object. now only supports 1 job//
class ThreadData {
	
public:
	IntermediateVec intermediateVec;
	
	// IntermediateVec curVec = {};

	const MapReduceClient& client;
	const InputVec& inputVec; 
	OutputVec& outputVec;
	JobState& jobState;
	const int tid;
	unsigned int& mapCounter; 
	vector<IntermediateVec>& sortedIntermediateVecs;
	ThreadData** thread_data_parr;
	const int multiThreadLevel;

	void mapCounterInc()
	{
		cout<<"inc mapCoutner THREADDATA"<<endl;
		mapCounter++;
	}
	unsigned int& getMapCounter() {return mapCounter;}
	ThreadData(	const MapReduceClient& client,
			   	const InputVec& inputVec, 
			  	OutputVec& outputVec,
			  	JobState& jobState, 
			  	const int tid,
			  	unsigned int& mapCounter,
			  	vector<IntermediateVec>& sortedIntermediateVecs,
			  	ThreadData** thread_data_parr,
			  	const int multiThreadLevel) : 
				client(client),
				inputVec(inputVec),
				outputVec(outputVec),
				jobState(jobState),
				tid(tid),
				mapCounter(mapCounter),
				sortedIntermediateVecs(sortedIntermediateVecs),
				thread_data_parr(thread_data_parr),
				multiThreadLevel(multiThreadLevel)
				{
					cout<<"ThreadData constructor "<<tid<<endl;
					cout<<"mapCoutner: "<<mapCounter<<endl;
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
	unsigned int mapCounter;

	void mapCounterInc()
	{
		cout<<"inc mapCoutner DATACONTEXT"<<endl;
		mapCounter++;
	}
	int getMapCounter() {return mapCounter;}

	ThreadData** thread_data_parr;	
	pthread_t* threads_arr;

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
		// cout<<"multiThreadLevel"<<multiThreadLevel<<endl;
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
													multiThreadLevel);
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




// per thread
// IntermediateVec intermediateVec;
// vector<IntermediateVec> sortedIntermediateVecs;
// IntermediateVec curVec = {};

// OutputVec _outputVec;

void printStatus(void* _context)
{
	ThreadData* context = (ThreadData *)_context; 
	JobState& jobState = context->jobState;

	cout<<"printStatus: Thread "<<context->tid<<endl;
	cout<<"jobState:"<<jobState.stage<<","<<jobState.percentage<<"%"<<endl;
	cout<<"intermediateVec:";
	// ThreadData thread;
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
	cout<<"emit2"<<endl;
	ThreadData* _context = (ThreadData*) context;
	_context->intermediateVec.push_back({key,value});
}

void emit3 (K3* key, V3* value, void* context){
	cout<<"emit3"<<endl;
	ThreadData* _context = (ThreadData*) context;
	// cout<<"here 160"<<endl;
	if (_context == nullptr){
		cout<<"nullptr!"<<endl;
		pthread_exit(nullptr);
	}
	_context->outputVec.push_back({key,value});
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
	unsigned int& mapCounter = context->getMapCounter(); 
	
	const MapReduceClient& client = context->client;
	const InputVec& inputVec = context->inputVec;
	
	
	// cout<<"here 160"<<endl;

	//start mapping phase:
	
	unsigned int inputSize = inputVec.size();
	// while(1);
	InputPair firstPair;
	cout<<"inputSize: "<<inputSize<<endl;
	cout<<"mapCoutner: "<<mapCounter<<endl;
	while(mapCounter < inputSize)
	{
	firstPair = inputVec[mapCounter];
	mapCounter++;
	cout<<"mapCoutner: "<<mapCounter<<endl;
	// cout<< "here 167"<<endl;
	// inputVec.erase(inputVec.begin());
	
	client.map(firstPair.first,firstPair.second,_context);
	
	jobState.percentage =100* (double(mapCounter) / double(inputSize));

	cout<<"here 174"<<endl;
	printStatus(_context);
	}	
	cout<<"here 177"<<endl;
	fflush(stdout);

	// return nullptr;
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
			cout<<"compkey is:"<<static_cast<KChar*>(compKeyp)->c<<endl;
			if (maxKeyp == nullptr) {maxKeyp = compKeyp; cout<<"max changed nullptr;"<<endl;}
			else if (*maxKeyp<*compKeyp) {maxKeyp=compKeyp; cout<<"max changed <;"<<endl;}	
		}
	}
	if (maxKeyp == nullptr){cout<<"maxkey is nullptr"<<endl;}
	else {cout<<"maxkey is: "<<static_cast<KChar*>(maxKeyp)->c<<endl;}
	return maxKeyp;

}
void doShuffle(void* _context){
	cout<<endl<<"entering shuffle phase"<<endl;
	ThreadData* context = (ThreadData*)_context;
	JobState& jobState = context->jobState; 
	vector<IntermediateVec>& sortedIntermediateVecs = context->sortedIntermediateVecs;

	printStatus(_context);

	jobState = {REDUCE_STAGE,0};

	K2* curKeyp = getMaxKey(_context);
	IntermediateVec curSortedVec;
	// IntermediateVec intermediateVec;
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
				
				// cout<<"breaking.."<<endl;

				break;
			}
			// usleep(10000);
			// cout<<"here 292"<<endl;
		}
		sortedIntermediateVecs.push_back(curSortedVec);
		curSortedVec.clear();
		curKeyp = getMaxKey(_context);

		// cout<<"curkey is:"<<static_cast<KChar*>(curKeyp)->c<<endl;
	}

	cout<<"done"<<endl;
	printStatus(_context);
	//get maxkey
}

void doShuffle2(void* _context){
	cout<<endl<<"entering shuffle phase"<<endl;
	ThreadData* context = (ThreadData*)_context;
	JobState& jobState = context->jobState; 
	vector<IntermediateVec>& sortedIntermediateVecs = context->sortedIntermediateVecs;


	printStatus(_context);


	jobState = {REDUCE_STAGE,0};

	K2* curKeyp;
	// IntermediateVec curIntermediateVec;
	IntermediateVec curSortedVec;
	IntermediateVec otherIntermediateVec;
	for (int i=0;i<context->multiThreadLevel;++i){
		// if intervec[i] is empty, continue to next
		
		IntermediateVec& curIntermediateVec = context->thread_data_parr[i]->intermediateVec;
		
		while (!(curIntermediateVec.empty())) {
			//else create new curSortedVec and add one pair form curIntermediateVec
			curSortedVec.push_back(curIntermediateVec.back());
			curIntermediateVec.pop_back();
			// get curKey	
			curKeyp = curSortedVec.back().first;
		cout<<"curkey is:"<<static_cast<KChar*>(curKeyp)->c<<endl;

			for (int j=0;j<context->multiThreadLevel;++j){
				cout<<"i,j="<<i<<","<<j<<endl;
				IntermediateVec &otherIntermediateVec = context->thread_data_parr[j]->intermediateVec;
				while(!(otherIntermediateVec.empty())){
					// go through all vectors - if key doesnt fit, continue to next
					// if it fits, move from interVec to curVec
					if(isEqual(otherIntermediateVec.back().first,curKeyp)){
						curSortedVec.push_back(otherIntermediateVec.back());
						otherIntermediateVec.pop_back();
					}
					else{
						cout<<"breaking"<<endl;
						break;
					}
				}
			}
		printStatus(_context);
			// when done checking all vecs, push curVec to sorted vecs and restart curVec	
			sortedIntermediateVecs.push_back(curSortedVec);
			curSortedVec.clear(); 	
		}
	}	
	cout<<"shuffle phase done"<<endl;
	printStatus(_context);
}

void *doJob(void* _context)
{
	cout<<"doJob"<<endl;
	doMap(_context);

	ThreadData* context = (ThreadData*)_context;
	JobState& jobState = context->jobState; 

	const MapReduceClient& client = context->client;
	// OutputVec& outputVec = context->outputVec;

	IntermediateVec& intermediateVec= context->intermediateVec;
	vector<IntermediateVec>& sortedIntermediateVecs = context->sortedIntermediateVecs;

	cout<<"entering sorting phase"<<endl;
	sort(intermediateVec.begin(),intermediateVec.end(),[](IntermediatePair p1,IntermediatePair p2)
		{return *(p1.first)<*(p2.first);});
	printStatus(_context);


	if (context->tid ==0){
		cout<<"yes!"<<endl;
		usleep(1000000);
		doShuffle(_context);
	}		

/*
		IntermediateVec curVec;
		curKeyp = intermediateVec.back().first;
		cout<<"curkey is:"<<static_cast<KChar*>(curKeyp)->c<<endl;

		for (int i=0;i<context->multiThreadLevel;++i){
			cout<<"shuffle. working on thread "<<i<<endl;
			intermediateVec= context->thread_data_parr[i]->intermediateVec;

			while(!intermediateVec.empty()){
				if (!isEqual(curKeyp,intermediateVec.back().first)){
					sortedIntermediateVecs.push_back(curVec);
					curVec.clear();
					curKeyp = intermediateVec.back().first;
				}

				curVec.push_back(intermediateVec.back());
				intermediateVec.pop_back();
			}

			sortedIntermediateVecs.push_back(curVec);
			curVec.clear();
			curKeyp = intermediateVec.back().first;
			// cout<<"curvecsize= "<< curVec.size()<<endl;
			// cout<<"sortedIntermediateVecs size: "<<sortedIntermediateVecs.size()<<endl;
			printStatus(_context);
		}
*/
	
	else{
		cout<<"NO!"<<endl;
		printStatus(_context);
		while(1);
	}
	
	printStatus(_context);
	cout<<"entering reduce phase"<<endl;

	
	// IntermediateVec nextVec;

	// client.reduce(&sortedIntermediateVecs.back(),nullptr);
	// sortedIntermediateVecs.pop_back();
	// cout<<"here"<<endl;
	// client.reduce(&sortedIntermediateVecs.back(),nullptr);
	
	while(!sortedIntermediateVecs.empty())
	{
		// nextVec = sortedIntermediateVecs.back();
		client.reduce(&sortedIntermediateVecs.back(),_context);
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
	cout<<"startMapReduceJob"<<endl;
	JobContext *context = new JobContext(client,inputVec,outputVec,multiThreadLevel);
	

	//init job object, currenly only 1 possible job, stored as global var.
	
	context->jobState = {MAP_STAGE,0};
	cout<<"mapCounter:"<<context->thread_data_parr[0]->getMapCounter()<<endl;
	// context->mapCounter = 0;
	// doJob((void*)context->thread_data_parr[0]);
	for (int i=0;i<multiThreadLevel;++i){
		pthread_create(&(context->threads_arr[i]),nullptr, &doJob,(void*)context->thread_data_parr[i]);
	}
	
	

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

void test2()
{
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

	IntermediatePair ip1 = {&a,&vc};
	IntermediatePair ip2 = {&i,&vc};

	K2* k1p = ip1.first;
	K2* k2p = ip2.first;
	

	K2* k3p = &b;
	K2* k4p = &d;

	bool res = *k3p<*k4p;
	bool res2 = b<*k3p;
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