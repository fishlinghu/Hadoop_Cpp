#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <algorithm>
#include <fstream>
#include <stdio.h>
#include <sstream>
#include <limits>

#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"

/*GRPC-server specific*/
using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

/*user .proto file specific*/
using masterworker::jobAssign; // this is a rpc-service
using masterworker::Master_to_Worker;
using masterworker::MasterQuery;
using masterworker::Worker_to_Master;

using namespace std;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);


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
            delete obj_CallData;
		}
        void set_mapper_output_filename(shared_ptr<BaseMapper> mapper, string s);
        void set_reducer_output_filename(shared_ptr<BaseReducer> reducer, string s);
        
        class CallData
            {
            public:
                // Take in the "service" instance (in this case representing an asynchronous
                // server) and the completion queue "cq" used for asynchronous communication
                // with the gRPC runtime.
                CallData(jobAssign::AsyncService* service, ServerCompletionQueue* cq)
                    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
                  // Invoke the serving logic right away.
                  Proceed();
                }
                Worker *parent;
                void Proceed();
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


	private: // for functions
	  void HandleRpcs();
	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string worker_ip_addr;
		std::unique_ptr<ServerCompletionQueue> cq_;
		jobAssign::AsyncService service_;
		std::unique_ptr<Server> worker_;
		/*have an object of CallData here..*/
		CallData* obj_CallData;

};

void Worker::set_mapper_output_filename(shared_ptr<BaseMapper> mapper, string s)
    {
    mapper->impl_->filename = s;
    }

void Worker::set_reducer_output_filename(shared_ptr<BaseReducer> reducer, string s)
    {
    reducer->impl_->filename = s;
    }

void Worker::HandleRpcs()
    {
    // Spawn a new CallData instance to serve new clients.
    obj_CallData = new CallData(&service_, cq_.get());
    obj_CallData->parent = this;
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) 
        {
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

void Worker::CallData::Proceed() {
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
            if (query_.map_reduce() == 1) { // mapper
                /*mapper code*/

                auto mapper = get_mapper_from_task_factory("cs6210");
                // mapper->impl_->filename = "foobar";
                /*pass one line at a time from the input file+offset to this function*/
                /*read it till the data-size; move pointer from offset till data-size*/
                /*the file to open: query_.file_path() <--- oprn this file in read mode
                with appropriate offset and data-size*/

                ifstream infile(query_.file_path());
                int data_size = query_.data_size();
                int file_offset = query_.file_offset();
                char* buffer = new char[data_size];
                // file operation
                infile.seekg(file_offset); // move the pointer to the offset
                infile.read(buffer, data_size); // put so many character into the buffer

                std::stringstream ss;
                ss.str(buffer); // this will convert it into buffer
                std::string line;

                string out_filename_for_master = query_.output_filename();
                string mapper_output_filename = out_filename_for_master + "_tmp";
                parent->set_mapper_output_filename(mapper, mapper_output_filename);

                while(std::getline(ss, line))
                    mapper->map(line); // this will call the emit() function internally
                                       // for a given line
                
                infile.close();
                ///////////////////////////////////////////////////////////////////////
                // sort single file.. name is given by the master in the field 'output_filename'
                // in the end we want to replace the unsorted file with the sorted file of the 
                // same name.
                vector<string> names;
                ifstream in(mapper_output_filename.c_str()); // file-pointer associated with input file
                fstream out; // file-pointer associated with output file
                out.open(out_filename_for_master.c_str(), fstream::out); // if not present create it.
                //out.open("WTF", fstream::out);
                if(!in.is_open())
                    cout << "Unable to open file" << endl;
                
                string word;
                while(getline(in, word))
                    names.push_back(word);

                sort(names.begin(), names.end());

                for (size_t i = 0; i < names.size(); ++i)
                    out << names[i] << '\n';

                // close the output and file
                out.close();
                in.close();
                //remove( mapper_output_filename.c_str() );
                // now remove the input file and rename output file
                // by input file
                /*int result= rename( out_filename.c_str() , in_filename.c_str() );
                if ( result == 0 )
                    puts ( "File successfully renamed" );
                else
                    perror( "Error renaming file" );    */
                /////////////////////////////////////////////////////////////
                /*set is_done*/
                reply_.set_is_done(true);
            } else if (query_.map_reduce() == 2) { // reducer code
                /*reducer code*/
                auto reducer = get_reducer_from_task_factory("cs6210");
                /*todo.....
                open a file; read a file and pass the parameter(key) from the file to the reducer*/
                /*have to understand: what is std::vector<std::string>*/
                /*replace the 'dummy' with real key...*/
                /*post it on piazza...if you don't understand*/
                // open the output file and start reading it from appropriate offset
                // both file name for opening the file and offset+data_size will be given
                // by the master to reduce worker.
                // this is the duty of 'master' to give appropriate offset and data-size at the entry/end 
                // of each key encountered.
                vector<string> value; // this holds the "1"s
                string key; // this holds the key
                string tempStr;
                // this is the cummilative sorted output file now.
                // ifstream fin(query_.output_filename()); // expect it to be in the same directory
                ifstream fin( "map_phase_output" ); // for testing
                
                int data_size = query_.data_size();
                int file_offset = query_.file_offset();
                
                fin >> key;
                while( fin >> tempStr )
                    {   
                    value.push_back( tempStr );
                    fin >> tempStr;
                    }

                fin.close();

                parent->set_reducer_output_filename(reducer, query_.output_filename()+"_reducer");

                // instead of passing each entry...just pass the whole vector of string which has each entry as "1"
                // number of entry is wrt how many times the key is repeated in the file.
                reducer->reduce(key, value);
                // examplar:
                // reducer->reduce("dummy"/*key*/, std::vector<std::string>({"1", "1"})/*all the values for the given key!!!!*/);    
                /*set is_done*/         
                reply_.set_is_done(true);
            }
        }
            ////////////////////////////////////////////////////////////////////////////

        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
        cout << parent->worker_ip_addr << " finished. " << endl;
      } else {
        GPR_ASSERT(status_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
    }


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

	//cout << "Coming from Worker::run()" << endl;
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
