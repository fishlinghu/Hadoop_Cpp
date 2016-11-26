#pragma once

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
		//Master master(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
		Master master;
		bool AssignTask();
		std::unique_ptr<jobAssign::Stub> stub_;
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) 
	{
	//Master master(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
	}

bool Master::AssignTask()
	{
	auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
	auto stub = masterworker::jobAssign::NewStub(channel);
	
	MasterQuery info;
    info.set_file_path("input");
    info.set_file_offset(5);
    info.set_map_reduce(1);
    info.set_data_size(1);
    info.set_id_assigned_to_worker(1);
    info.set_output_filename("output");
    
    Master_to_Worker request;
    request.set_masterquery( info );
    
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


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() 
	{
	// Assign map tasks to worker
	bool flag = master.AssignTask();
	// Collect the result
	// Assign reduce tasks to worker
	// Collect the result
	return true;
	}