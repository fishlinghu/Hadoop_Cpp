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

    ifstream in(argv[1]);
    ofstream out;
    out.open("sort_output.txt");

    if(!in.is_open())
        cout << "Unable to open file\n";

    
    string word;
    while(getline(in, word)){
        // cout << "word: " << word << endl;
        names.push_back(word);
    }
    sort(names.begin(), names.end());

    // Loop to print names
    // for (size_t i = 0; i < names.size(); i++)
    //     cout << names[i] << '\n';

    for (size_t i = 0; i < names.size(); ++i)
        out << names[i] << '\n';

    out.close();
    in.close();
    return 0;
}