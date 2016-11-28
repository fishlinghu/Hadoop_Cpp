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

	while (inFile >> firstWord)
	{
	    cout << firstWord << endl;
	    inFile.ignore(numeric_limits<streamsize>::max(), '\n');
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