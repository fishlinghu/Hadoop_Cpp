#include <stdio.h>
#include <sstream>
#include <iostream>
#include <limits>
#include <fstream>

using namespace std;
int main(int argc, char const *argv[])
{
	ifstream inFile(argv[1]);
	string firstWord;


	// while (inFile >> firstWord)
	// {
	//     cout << firstWord << endl;
	//     inFile.ignore(numeric_limits<streamsize>::max(), '\n');
	// }

	string line;
	while(getline(inFile, line)) {
		string dlim = " ";
		string token = line.substr(0, line.find(dlim));
		cout << "token: " << token << endl;
	}



	inFile.seekg(0, ios::beg);
	inFile.seekg(10);
	char* buffer = new char[40];
	inFile.read(buffer, 40);

	std::stringstream ss;
	ss.str(buffer);
	std::string firstWord1;
	cout<< "////////////////////////////"<< endl;
	string s;
	while(getline(ss, s, ' ')) {
		cout << "s: " << s << endl;
	}
	while (ss >> firstWord1)
	{
	    cout << firstWord1 << endl;
	    ss.ignore(numeric_limits<streamsize>::max(), '\n');
	}

	return 0;
}

  // istringstream iss("This is a string\n And this is\n");
  // string s;
  // while ( getline( iss, s ) ) {
  // 	cout << "1" << endl;
  // 	getline( iss, s, ' ' );
  // 	cout << "iss: " << iss << endl;
  // 	cout << "s: " << s << endl;
  // }
// }