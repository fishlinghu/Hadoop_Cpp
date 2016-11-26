#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <memory>
#include <iostream>
#include <string>
#include <thread>

#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"

/*GRPC-server specific*/
using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

/*GRPC-client specific*/
// Discuss with cliff: I don't think we need to use this as
// 'Worker' is behaving as 'server', however master may need to
// may need to use this as 'master' is a client.
// using grpc::Channel; // This channel is for the store to communicate to vendor
// using grpc::ClientAsyncResponseReader;
// using grpc::ClientContext;
// using grpc::CompletionQueue;

/*user .proto file specific*/
using masterworker::jobAssign; // this is a rpc-service
using masterworker::Master_to_Worker;
using masterworker::MasterQuery;
using masterworker::Worker_to_Master;

using namespace std;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* Creating a new CallData class and initantiating its object inside Worker class */
////////////////////////////////////////////////////////////
class CallData {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    CallData(jobAssign::AsyncService* service, ServerCompletionQueue* cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      // Invoke the serving logic right away.
      Proceed();
    }

    void Proceed() {
      if (status_ == CREATE) {
        // Make this instance progress to the PROCESS state.
        status_ = PROCESS;

        // As part of the initial CREATE state, we *request* that the system
        // start processing SayHello requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.
        service_->RequestAssignTask(&ctx_, &request_, &responder_, cq_, cq_,
                                  this);
      } else if (status_ == PROCESS) {
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        new CallData(service_, cq_);

        // The actual processing.
        ///////////////////////////////////////////////////////////////////////////
        // use your own function here... 		
        unsigned int mapR;
        int size = request_.masterquery_size();
        for (int i = 0; i < size; ++i) // parse through all the queries in the queue
        {
	        query_ = request_.masterquery(i);
	        if (query_.map_reduce() == 1) {
	        	/*mapper code*/
        		auto mapper = get_mapper_from_task_factory("cs6210");
				mapper->map("I m just a 'dummy', a \"dummy line\"");
	        } else if (query_.map_reduce() == 2) {
	        	/*reducer code*/
				auto reducer = get_reducer_from_task_factory("cs6210");
				reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));	        	
	        }
        }
        ////////////////////////////////////////////////////////////////////////////

        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
      } else {
        GPR_ASSERT(status_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
    }

   private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    jobAssign::AsyncService* service_;
    // The producer-consumer queue where for asynchronous server notifications.
    ServerCompletionQueue* cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx_;

    // What we get from the client.
    Master_to_Worker request_;
    // What we send back to the client.
    Worker_to_Master reply_;
    // holds the masterQuery
    MasterQuery query_;
    // The means to get back to the client.
    ServerAsyncResponseWriter<Worker_to_Master> responder_;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;  // The current serving state.
  };
/////////////////////////////////////////////////////

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

		/*Adding distructor*/
		~Worker() {
			worker_->Shutdown();
			// Always shutdown the completion queue after the server.
			cq_->Shutdown();
		}


	private: // for functions
	  void HandleRpcs() {
	    // Spawn a new CallData instance to serve new clients.
	    obj_CallData = new CallData(&service_, cq_.get());
	    void* tag;  // uniquely identifies a request.
	    bool ok;
	    while (true) {
	      // Block waiting to read the next event from the completion queue. The
	      // event is uniquely identified by its tag, which in this case is the
	      // memory address of a CallData instance.
	      // The return value of Next should always be checked. This return value
	      // tells us whether there is any kind of event or cq_ is shutting down.
	      GPR_ASSERT(cq_->Next(&tag, &ok));
	      GPR_ASSERT(ok);
	      static_cast<CallData*>(tag)->Proceed();
	    }
	  }
	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string worker_ip_addr;
		std::unique_ptr<ServerCompletionQueue> cq_;
		jobAssign::AsyncService service_;
		std::unique_ptr<Server> worker_;
		/*have an object of CallData here..*/
		CallData* obj_CallData;

};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) { // "Constructor"
	worker_ip_addr = ip_addr_port;
}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of "how you will call map and reduce"
		Remove them once you start writing your own logic */ 
	// Big while loop
	// Worker should keep continue spinning; 
	// if you receive a job from the master, (whatever job worker need to do,) worker with start with the job
	// Use the worker to receive the message from the master 
	// and once it receive the message it should reply to the message to make sure communication is ok

	// std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));

	/* I think depending on the message in the queue, whether
	   'map_reduce' is true(map) or false(reduce), we will call the 
	   functions accordingly */
	cout << "Coming from Worker::run()" << endl;
	std::string server_address(worker_ip_addr);

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    worker_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    HandleRpcs();


	return true;
}
