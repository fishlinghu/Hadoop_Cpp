#pragma once

#include <string>
#include <iostream>
#include <fstream>

using namespace std;

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string filename;
		//void input_filename(string s);
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() 
	{
	filename = "NoChange";
	}

/*inline void BaseMapperInternal::input_filename(string s)
	{
	filename = s;
	}*/

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	ofstream file;
    file.open (filename.c_str(), ios_base::out | ios_base::app); 
    // filename should be unique for each worker
    // so we should assign each worker with an unique ID
    // in that way, we dont need to communicate with the master to tell it the path of temporary file
    // the master can find the file using, for example, "2.txt", and 2 is the id of a certain worker
    file << key << " " << val << endl;

    file.close();
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string filename;
		// void input_filename(string s);
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
	filename = "NoChange";
}

/*inline void BaseReducerInternal::input_filename(string s)
	{
	filename = s;
	}*/


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	ofstream file;
    file.open (filename.c_str(), ios_base::out | ios_base::app); 
    file << key << " " << val << endl;

    file.close();
}
