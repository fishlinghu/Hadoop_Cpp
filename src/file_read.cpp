#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <iterator>
using namespace std;

int main(int argc, char const *argv[])
{

    // Empty vector holding all key-value pair of file
    vector<string> names;

    ifstream in(argv[1]);

    if(!in.is_open())
        cout << "Unable to open file\n";

    in.seekg(10);
    int len = 20;
    char* buffer = new char [len];

    in.read(buffer, len); // put len character after seek, into buffer.
    string word;
    while(getline(in, word)){
        cout << /*"word: " <<*/ word << endl;
        // names.push_back(word);
        // in >> word;
    }

    string buffer_str(buffer, buffer+len);
    cout << "buffer_str: " << buffer_str;


	//////////////////////////////////////////////////////
	printf("\n/////////////////////////////\n");


    std::stringstream ss;
    ss.str(buffer);
    std::string item;
    // string delim = "\n";
    while (std::getline(ss, item/*, delim*/)) {
        // elems.push_back(item);
        cout << item << endl;
    }

    in.close();
    return 0;
}