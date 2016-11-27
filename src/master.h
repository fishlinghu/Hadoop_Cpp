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
		explicit Master(std::shared_ptr<Channel> channel)
    		: stub_(jobAssign::NewStub(channel)) {}
		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		// store MapReduceSpec and FileShard here
		//Master master;
		bool AssignTask();
		void sort_and_write();
		bool check_end(vector<bool> &input);
		std::unique_ptr<jobAssign::Stub> stub_;
		int num_of_worker;
		int num_of_file_shard;
		vector<string> map_output_filename_vec;
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) 
	{
	//Master master(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
	num_of_file_shard = 1;
	int i = 0;
	ostringstream sstream;

	while(i < num_of_file_shard)
		{	
		sstream.str("");
		sstream << i;
		map_output_filename_vec.push_back( sstream.str() );
		++i;
		}
	}

bool Master::AssignTask()
	{
	auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
	auto stub = masterworker::jobAssign::NewStub(channel);
	
	Master_to_Worker request;
	MasterQuery* info;
	info = request.add_masterquery();
    info->set_file_path("input");
    info->set_file_offset(5);
    info->set_map_reduce(1);
    info->set_data_size(1);
    info->set_id_assigned_to_worker(1);
    info->set_output_filename("output");
    
    // Container for the data we expect from the server.
    Worker_to_Master reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq;

    // Storage for the status of the RPC upon completion.
    Status status;

    // stub_->AsyncSayHello() performs the RPC call, returning an instance we
    // store in "rpc". Because we are using the asynchronous API, we need to
    // hold on to the "rpc" instance in order to get updates on the ongoing RPC.
    std::unique_ptr<ClientAsyncResponseReader<Worker_to_Master> > rpc(stub_->AsyncAssignTask(&context, request, &cq));

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the integer 1.
    rpc->Finish(&reply, &status, (void*)1);
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or the cq_ is shutting down.
    GPR_ASSERT(cq.Next(&got_tag, &ok));

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

bool Master::check_end(vector<bool> &input)
	{
	int i = 0;
	while(i < input.size())
		{	
		if(input[i] == true)
			return true;
		++i;
		}
	return false;
	}

void Master::sort_and_write()
	{
	ofstream fout("output");

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
			fileEnd[i] = true;
			}
		++i;
		}

	string tempStr;
	string oldStr;
	int idx;
	while( check_end( fileEnd ) )
		{	
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

	i = 0;
	while(i < num_of_file_shard)
		{	
		ifs[i]->close();
		++i;
		}

	fout.close();
	return;

	}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() 
	{
	// Assign map tasks to worker
	Master master(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
	bool flag = master.AssignTask();
	cout << "flag: " << flag <<endl;
	// Collect the result
	sort_and_write();
	// Assign reduce tasks to worker
	// Collect the result
	return true;
	}