#pragma once

#include <vector>
#include <map>
#include "mapreduce_spec.h"


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
    // What do we need to record?
    // offset of shard
    // corresponding node ID? use a map
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	// record the offset in file
    // sizeof(char) == 1
    // split the file into parts of equal parts
    // size should be map_kilobytes*1000 chars
    int i = 0;
    FILE* f;
    long long int fileSize, offset;
    while(i < mr_spec.input_file_name.size())
        {   
        // Could be multiple files
        f = fopen( mr_spec.input_file_name[i].c_str() ,"r");
        fseek(f, 0, SEEK_END);
        fileSize = ftell(f);
        offset = 0;
        while(offset < fileSize)
            {   
            // record the offset in some data structure
            offset = offset + mr_spec.map_kilobytes * 1000;
            }
        fclose(f);
        ++i;
        }
    return true;
}
