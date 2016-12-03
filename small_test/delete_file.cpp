#include <stdio.h>
#include <string>
#include <iostream>

using namespace std;

int main(int argc, char const *argv[])
{
	string file(argv[1]);

	if( remove( file.c_str() ) != 0 )
	perror( "Error deleting file" );
	else
	puts( "File successfully deleted" );

	return 0;
}