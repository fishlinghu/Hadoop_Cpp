#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <stdlib.h> // for calloc()
#include <fstream> // for file handling 

using namespace std;

/*Global data structure here*/
struct MapReduceSpec {
	// 
	std::vector<string> input_file_name; // file name 'input/testdata_1.txt'
	struct name // for a given worker
	{
		string ipaddr;
		string ports;
	};
	std::vector<name> ipaddr_port_list;
	int n_workers, n_output_files, map_kilobytes;
	string output_dir;
	string user_id;
};

int main(int argc, char const *argv[])
{
	/*parse the config.ini file here*/
	MapReduceSpec* mr_spec;
	ifstream config;
	string line;
	mr_spec = (MapReduceSpec*)calloc(1, sizeof(MapReduceSpec));
	if(argc > 1){
	  	config.open(argv[1]);
	} else {
		cout << "Exiting abnormally.. didn't provide the config file" << endl;
		exit(-1);
	}

	string ipaddr;
	string ports;		
	// Read the file here...
	while(!(config.eof()/* || line.empty())*/)){		
		config >> line;
		cout << line << endl;
		// Populate the structures of MapReduceSpec here
		std::string s = line;
		std::string delimiter = "=";
		std::string token = s.substr(0, s.find(delimiter));
		cout << token << endl;
		if(token.compare("n_workers") == 0) {
			s.erase(0, s.find(delimiter) + delimiter.length());
			token = s.substr(0, s.find(delimiter));
			mr_spec->n_workers = stoi(token, nullptr);
		}
		if(token.compare("worker_ipaddr_ports") == 0) {
			mr_spec->ipaddr_port_list.reserve(20);
			s.erase(0, s.find(delimiter) + delimiter.length());
			token = s.substr(0, s.find(delimiter));
			cout << "token: " << token << endl;
			std::string dlim1 = ",";
			//////////////////////////////////////////////////////
			size_t pos = 0;
			std::string token1;
			int it = 0;
			while ((pos = token.find(dlim1)) != std::string::npos) {
			    token1 = token.substr(0, pos);
			    std::cout << token1 << std::endl;
			    ipaddr = token1.substr(0, (token1.find(":")));
			    cout << token1.substr(0, (token1.find(":"))) << endl;
			    token1.erase(0, (token1.find(":") + 1));
			    ports = token1;
			    cout << token1 << endl;
			    token.erase(0, pos + dlim1.length());
			    mr_spec->ipaddr_port_list[it].ipaddr = ipaddr;
			    mr_spec->ipaddr_port_list[it].ports = ports;
			    it++;
			}
			// TOo take care of the corner case
			std::cout << "token: " << token << std::endl;
			ipaddr = token.substr(0, (token.find(":")));
		    token.erase(0, (token.find(":") + 1));
		    ports = token;
		    mr_spec->ipaddr_port_list[it].ipaddr = ipaddr;
		    mr_spec->ipaddr_port_list[it].ports = ports;
		}
		if(token.compare("input_files") == 0) {
			mr_spec->input_file_name.reserve(20);
			s.erase(0, s.find(delimiter) + delimiter.length());
			token = s.substr(0, s.find(delimiter));
			cout << "token: " << token << endl;
			std::string dlim1 = ",";
			//////////////////////////////////////////////////////
			size_t pos = 0;
			std::string token1;
			std::vector<int>::iterator it;
			// int it = 0;
			while ((pos = token.find(dlim1)) != std::string::npos) {
			    token1 = token.substr(0, pos);
			    std::cout << "token1: " << token1 << std::endl;
			    token.erase(0, pos + dlim1.length());
			    cout << *it << endl;
			    mr_spec->input_file_name.insert(it, token1);
			    it++;
			}
			    cout << *it << endl;
		    mr_spec->input_file_name.insert(it, token);
		    cout << mr_spec->input_file_name.size() << endl;
		}
		if(token.compare("output_dir") == 0) {
			s.erase(0, s.find(delimiter) + delimiter.length());
			token = s.substr(0, s.find(delimiter));
			mr_spec->output_dir = token;
		}
		if(token.compare("n_output_files") == 0) {
			s.erase(0, s.find(delimiter) + delimiter.length());
			token = s.substr(0, s.find(delimiter));
			mr_spec->n_output_files = stoi(token, nullptr);
		}
		if(token.compare("map_kilobytes") == 0) {
			s.erase(0, s.find(delimiter) + delimiter.length());
			token = s.substr(0, s.find(delimiter));
			mr_spec->map_kilobytes = stoi(token, nullptr);
		}
		if(token.compare("user_id") == 0) {
			s.erase(0, s.find(delimiter) + delimiter.length());
			token = s.substr(0, s.find(delimiter));
			mr_spec->user_id = token;			
		}
		token = s.substr(0, s.find(delimiter));
		cout << token << endl;
	}

	//////////////////////////////////
	// Reading the structure:
	cout << mr_spec->input_file_name.size() << endl;
	for (int i = 0; i < 2; ++i)
	{
		cout << mr_spec->input_file_name[i] << endl;
	}



	// Ending program gracefully
  	config.close();
	free(mr_spec);
	return 0;
}				