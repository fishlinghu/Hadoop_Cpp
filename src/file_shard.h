#pragma once

#include <vector>
#include <map>
#include "mapreduce_spec.h"


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard 
    {
    // What do we need to record?
    // offset of shard
    // corresponding node ID? use a map
    string input_filename;
    long long int offset;
    long long int dataSize;
    };


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	// record the offset in file
    // sizeof(char) == 1
    // split the file into parts of equal parts
    // size should be map_kilobytes*1000 chars
    long long int i, j;
    char ch;

    FILE* f;
    long long int dataSize, fileSize, offset, lastoffset;
    vector<long long int> offset_vec;
    vector<long long int> dataSize_vec;
    FileShard new_fileInfo;

    dataSize = mr_spec.map_kilobytes * 1000;
    i = 0;
    while(i < mr_spec.input_file_name.size())
        {   
        // Could be multiple files
        f = fopen( mr_spec.input_file_name[i].c_str() ,"r");
        fseek(f, 0, SEEK_END);
        fileSize = ftell(f); // get the file size
        
        offset = 0;
        fseek(f, 0, SEEK_SET);
        offset_vec.push_back( 0 );
        lastoffset = 0;
        
        offset = offset + dataSize;
        while(offset < fileSize)
            {   
            fseek(f, offset, SEEK_SET);
            // move to none space char
            ch = getc(f);
            while(offset < fileSize && ch != ' ')
                {   
                offset = offset + 1;
                ch = getc(f);
                }
            // now ch is the end of file or space
            // cout << "offset:" << offset << endl;
            while(offset < fileSize && ch == ' ')
                {   
                offset = offset + 1;
                ch = getc(f);
                }   
            // now ch is the end of file or start of a word
            
            // cout << "offset:" << offset << endl;
            if(offset != fileSize)
                {
                dataSize_vec.push_back( offset-lastoffset );
                offset_vec.push_back( offset );
                lastoffset = offset;
                }
            offset = offset + dataSize;
            }
        dataSize_vec.push_back( fileSize-lastoffset );
        // offset and data size information of the file
        j = 0;
        while(j < offset_vec.size())
            {   
            new_fileInfo.input_filename = mr_spec.input_file_name[i];
            new_fileInfo.offset = offset_vec[j];
            new_fileInfo.dataSize = dataSize_vec[j];
            fileShards.push_back( new_fileInfo );
            ++j;
            }
        offset_vec.clear();
        dataSize_vec.clear();
        fclose(f);
        ++i;
        }
    return true;
}
