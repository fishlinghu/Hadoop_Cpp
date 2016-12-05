#include <iostream>
#include <vector>
#include <list>
#include <map>
#include <algorithm>
#include <thread>

// #include "thread_try1.cpp"

using namespace std;

class thread_try1
{
private:
	// thread third;
public:
	// thread_try1() {
	// 	third = std::thread(&thread_try1::func1, this/*&obj1*/);
	// };
	// ~thread_try1();
void func1() {
		cout << "this is thread_try1::func1()" << endl;
	}
};



void func1(void) {
    cout << endl << "this is func1" << endl;
}

int fact(int n) {
	cout << "this is fact()" << endl;
    if(n == 1) {
        return 1;
    }else{
            return(n*fact(n-1));
       }
}

int main() {

	thread_try1 obj1;

	// obj1.func1(); // 

	std::thread first; 
	std::thread second; 
	std::thread third; // this calls the class function...


	first = std::thread (func1); // spwans a new thread that calls first()
	second = std::thread (fact, 2);
	// third = std::thread(obj1.func1);
	third = std::thread(&thread_try1::func1, &obj1);
// 	cout << "factorial of 5 is: " << second << endl; // spwans a new thread that calls fact(n)
	
	cout << "'func1()' and 'fact(n)' now execute concurrently...." << endl;
	
	first.join();
	second.join();
	third.join();
	
	cout << "both functions now finished execution" << endl;
	
	return 0;
}
