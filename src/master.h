#pragma once
#include <string>
#include <fstream>
#include <sstream>
#include <vector>
#include "mapreduce_spec.h"
#include "file_shard.h"


#include <grpc++/grpc++.h>

#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using masterworker::Master_to_Worker;
using masterworker::MasterQuery;
using masterworker::Worker_to_Master;
using masterworker::jobAssign;

using namespace std;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);
		
		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		// store MapReduceSpec and FileShard here
		class MasterGRPC
			{
			public:
				explicit MasterGRPC(std::shared_ptr<Channel> channel, Master* ptr)
    				: stub_(jobAssign::NewStub(channel)) {caller = ptr;}
    			void AssignTask(int map_or_reduce, int task_id);
    			bool Check_result();
    			std::unique_ptr<jobAssign::Stub> stub_;
    			// std::unique_ptr<ClientAsyncResponseReader<Worker_to_Master> > rpc;

    			// Context for the client. It could be used to convey extra information to
				// the server and/or tweak certain RPC behaviors.
				// ClientContext context;
				ClientContext* contextPtr;

    			Master* caller;
    			// Container for the data we expect from the server.
			    Worker_to_Master reply;

			    // The producer-consumer queue we use to communicate asynchronously with the
			    // gRPC runtime.
			    CompletionQueue cq;

				// Storage for the status of the RPC upon completion.
			    Status status;
			};

		vector<MasterGRPC*> connection_vec;

		void hi_worker();
		void run_map();
		void sort_and_write();
		bool check_end(vector<bool> &input);
		
		int num_of_worker;
		int num_of_file_shard;

		vector<string> map_output_filename_vec;
		
		vector<string> worker_IP_vec;
		vector<string> worker_port_vec;
		vector<string> input_file_name;

		vector<string> reducer_input_filename_vec; // populate this vector in sort_and_write()
		
		vector<FileShard> file_shards_vec;
		
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) 
	{
	num_of_worker = mr_spec.n_workers;
	int i;

	cout << "# of file shards: " << file_shards.size() << endl;
	num_of_file_shard = file_shards.size();

	i = 0;
	while(i < file_shards.size())
		{	
		map_output_filename_vec.push_back( to_string(i) );
		file_shards_vec.push_back( file_shards[i] );
		++i;
		}

	i = 0;
	while(i < num_of_worker)
		{	
		// cout << mr_spec.ipaddr_port_list[i].ipaddr << endl;
		worker_IP_vec.push_back( mr_spec.ipaddr_port_list[i].ipaddr );
		worker_port_vec.push_back( mr_spec.ipaddr_port_list[i].ports );
		++i;
		}

	i = 0;
	while(i < mr_spec.input_file_name.size())
		{	
		input_file_name.push_back( mr_spec.input_file_name[i] );
		++i;
		}
	}

void Master::hi_worker()
	{
	MasterGRPC* obj;
	string addr;
	int i = 0;
	while(i < num_of_worker)
		{
		addr = worker_IP_vec[i] + ":" + worker_port_vec[i];
		obj = new MasterGRPC( grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()), this );
		connection_vec.push_back( obj );
		++i;
		}
	}

void Master::MasterGRPC::AssignTask(int map_or_reduce, int task_id) //1 for map, 2 for reduce
	{
	//auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
	//auto stub = masterworker::jobAssign::NewStub(channel);
	
	// Num of request = num of file shards
	Master_to_Worker request;
	MasterQuery* info;
	info = request.add_masterquery();
    info->set_file_path( caller->file_shards_vec[task_id].input_filename ); //<---the name of input file
    info->set_file_offset( caller->file_shards_vec[task_id].offset );
    info->set_map_reduce( map_or_reduce );
    info->set_data_size( caller->file_shards_vec[task_id].dataSize ); //<--
    info->set_id_assigned_to_worker(1);
    info->set_output_filename(caller->map_output_filename_vec[task_id]); //<---the name of output file
    
    

    // stub_->AsyncSayHello() performs the RPC call, returning an instance we
    // store in "rpc". Because we are using the asynchronous API, we need to
    // hold on to the "rpc" instance in order to get updates on the ongoing RPC.
    //rpc = move( stub_->AsyncAssignTask(&context, request, &cq) );
    // *context = nullptr;
    contextPtr = new ClientContext(); // remember to delete it somewhere
    std::unique_ptr<ClientAsyncResponseReader<Worker_to_Master> > rpc( stub_->AsyncAssignTask(contextPtr, request, &cq) );

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the integer 1.
    rpc->Finish(&reply, &status, (void*)1);
	}

bool Master::MasterGRPC::Check_result()
	{
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or the cq_ is shutting down.
    //GPR_ASSERT(cq.Next(&got_tag, &ok));
    
    //cq.Next(&got_tag, &ok);
    cq.AsyncNext(&got_tag, &ok, std::chrono::system_clock::now()+std::chrono::seconds(3));

    // Verify that the result from "cq" corresponds, by its tag, our previous
    // request.
    GPR_ASSERT(got_tag == (void*)1);
    // ... and that the request was completed successfully. Note that "ok"
    // corresponds solely to the request for updates introduced by Finish().
    GPR_ASSERT(ok);

    // Act upon the status of the actual RPC.
    if (status.ok()) 
        {
        return reply.is_done();
        } 
    else 
        {
        return false;
        }
	}

/*void Master::run_map()
	{
	int i = 0;
	bool flag;
	while(i < num_of_worker)
		{
		connection_vec[i]->AssignTask(1, i);
		//flag = connection_vec[i]->Check_result();
		//cout << "i:" << i << ", " << flag << endl;
		++i;
		}
	i = 0;
	while(i < num_of_worker)
		{	
		flag = connection_vec[i]->Check_result();
		cout << "i:" << i << ", " << flag << endl;
		delete connection_vec[i]->contextPtr;
		++i;
		}
	}*/

void Master::run_map()
	{
	vector<bool> worker_busy(num_of_worker, false); // record which worker is busy / available for a task

	int i, task_finished = 0;

	int task_remain = num_of_file_shard - 1; // -1 because it is used as the index which starts from 0
	while( task_remain >= 0 )
		{	
		i = 0;
		while(i < num_of_worker)
			{	
			if(worker_busy[i] == false)
				{	
				connection_vec[i]->AssignTask(1, task_remain);
				worker_busy[i] = true;
				--task_remain;
				}
			++i;
			}
		// now all workers are busy, or all tasks are assigned
		i = 0;
		while(i < num_of_worker)
			{	
			if( worker_busy[i] == true )
				{	
				if( connection_vec[i]->Check_result() == true )
					{	
					// worker i is available for a nex task
					worker_busy[i] = false;
					++task_finished;
					}
				}
			++i;
			}
		}

	while(task_finished < num_of_file_shard)
		{	
		i = 0;
		while(i < num_of_worker)
			{	
			if( worker_busy[i] == true )
				{	
				if( connection_vec[i]->Check_result() == true )
					{
					worker_busy[i] = false;
					++task_finished;	
					}
				}
			++i;
			}
		}
	}

bool Master::check_end(vector<bool> &input)
	{
	int i = 0;
	while(i < input.size())
		{
		//cout << input[i] << endl;
		if(input[i] == false)
			{
			//cout << "TRUE" << endl;
			return false;
			}
		++i;
		}
	return true;
	}

void Master::sort_and_write()
	{
	// merge multiple files from mapper into one big file
	ofstream fout("map_phase_output");//<--- input file for reducer
	// add offset in the future
	//cout << num_of_file_shard << "!!" << endl;
	vector <ifstream *> ifs;

	ifstream *ptr;

	int i = 0;
	while(i < num_of_file_shard)
		{	
		ptr = new ifstream();
		ifs.push_back(ptr);
		ifs[i]->open( map_output_filename_vec[i].c_str() );
		++i;
		}

	vector<string> buf_key( num_of_file_shard );
	vector<string> buf_val( num_of_file_shard );
	vector<bool> fileEnd( num_of_file_shard, false );

	i = 0;
	while (i < num_of_file_shard)
		{	
		if(*(ifs[i]) >> buf_key[i])
			{
			*(ifs[i]) >> buf_val[i];
			}
		else
			{	
			cout << "Cannot read file: " << i << endl;
			fileEnd[i] = true;
			}
		++i;
		}

	string tempStr;
	string oldStr;
	int idx;
	//cout << check_end( fileEnd ) << endl;
	while( check_end( fileEnd ) == false )
		{	
		//cout << "HI" << endl;
		// chose the file to read from by comparing the strings
		tempStr = "";
		i = 0;
		while(i < num_of_file_shard)
			{	
			if(fileEnd[i] == false)
				{	
				if(tempStr.empty() == true || tempStr.compare(buf_key[i]) > 0)
					{	
					tempStr = buf_key[i];
					idx = i;
					}
				}
			++i;
			}
		// now we decide to read from ifs[idx]
		// oldStr is the word we want to write in this round
		oldStr = buf_key[idx];
		cout << oldStr << endl;
		fout << buf_key[idx] << " " << buf_val[idx] << endl;
		while( *(ifs[idx]) >> buf_key[idx] && oldStr.compare( buf_key[idx] ) == 0 )
			{
			*(ifs[idx]) >> buf_val[idx];
			fout << buf_key[idx] << " " << buf_val[idx] << endl;
			}
		if( oldStr.compare( buf_key[idx] ) == 0 )
			{
			// we reach the end of ifs[idx]
			fileEnd[idx] = true;
			}
		else
			{	
			*(ifs[idx]) >> buf_val[idx];
			}
		// else, the buf[idx] contains the next word we are going to read from ifs[idx]
		}
	//cout << "End loop" << endl;

	i = 0;
	while(i < num_of_file_shard)
		{	
		ifs[i]->close();
		//remove( map_output_filename_vec[i].c_str() );
		++i;
		}
	fout.close();
	return;

	}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() 
	{
	// Establish connection between master and many workers
	hi_worker();
	//MasterGRPC master(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()), this);
	//MasterGRPC master2(grpc::CreateChannel("localhost:50052", grpc::InsecureChannelCredentials()), this);
	//bool flag1 = master.AssignTask(1, 0);
	//bool flag2 = master.AssignTask(1, 1);
	//cout << "flag: " << flag1 <<endl;
	//cout << "flag: " << flag2 <<endl;
	run_map();
	// Collect the result
	sort_and_write();
	// Assign reduce tasks to worker
	//flag1 = master.AssignTask(2, 0);
	//flag2 = master.AssignTask(2, 1);
	// Collect the result
	return true;
	}