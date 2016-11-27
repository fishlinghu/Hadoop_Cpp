#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
using namespace std;

int main(int argc, char const *argv[])
{

    // Empty vector holding all key-value pair of file
    vector<string> names;
    string in_filename(argv[1]);

	
    string out_filename = in_filename + "_tmp";

    ifstream in(in_filename); // file-pointer associated with input file
    fstream out; // file-pointer associated with output file
    out.open(out_filename, fstream::out); // if not present create it.

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
    // now remove the input file and rename output file
    // by input file
    int result= rename( out_filename.c_str() , in_filename.c_str() );
    if ( result == 0 )
        puts ( "File successfully renamed" );
    else
        perror( "Error renaming file" );    
    

    return 0;
}