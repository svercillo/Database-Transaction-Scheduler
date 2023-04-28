#include <fstream>
#include <sstream>
#include <iostream>

#include "transactions_parser.h"
#include "cascadeless_scheduler.h"
#include "recoverable_scheduler.h"
#include "serial_scheduler.h"
#include "serializable_scheduler.h"

using namespace std;

std::string get_input_string(char * file_name){
    // convert input file to string
    string content;
    ifstream file(file_name);

    if (file) {
        stringstream buffer;
        buffer << file.rdbuf();
        content = buffer.str();
        file.close();
    } else {
        cout << "Error: unable to open file." << endl;
        abort();
    }
    return content;   
}

void dump_output_file(string file_name, std::string contents)
{
    
    ofstream fw(file_name, std::ofstream::out);
    if (fw.is_open())
        fw << contents;

    fw.close();
}


string get_output_file_name(string input_file){
    string output_file = input_file.substr(0, input_file.find_last_of(".")) + "_output.txt";
    return output_file;
}

int main(int argc, char ** argv) {

    char *input_file_name = argv[1];
    string output_file_name = get_output_file_name(string(input_file_name));

    string bonus_str;
    if (argc == 3)
        bonus_str = std::string(argv[2]);

    bool is_bonus = bonus_str == "TRUE";


    ifstream f(input_file_name);

    std::string input_contents = get_input_string(input_file_name);

    TransactionsParser * parser = new TransactionsParser(input_contents);
    CascadelessScheduler * cascadeless_scheduler;
    RecoverableScheduler * recoverable_scheduler;
    SerialScheduler * serial_scheduler;
    SerializableScheduler * serializable_scheduler;


    string res = "";
    vector<const Action *> actions_vec = parser->actions_vec;


    if (is_bonus){
        serial_scheduler = new SerialScheduler(actions_vec);
        serial_scheduler->schedule_tasks();
        res += serial_scheduler->to_string();
        std::cout << serial_scheduler->to_string() << endl;
        delete serial_scheduler;

        serializable_scheduler = new SerializableScheduler(actions_vec);
        serializable_scheduler->schedule_tasks();
        res += serializable_scheduler->to_string();
        std::cout << serializable_scheduler->to_string() << endl;
        delete serializable_scheduler;
        
    }

    cascadeless_scheduler = new CascadelessScheduler(actions_vec);
    cascadeless_scheduler->schedule_tasks();
    res += cascadeless_scheduler->to_string();
    std::cout << cascadeless_scheduler->to_string() << endl;
    delete cascadeless_scheduler;
    
    recoverable_scheduler = new RecoverableScheduler(actions_vec);
    recoverable_scheduler->schedule_tasks();
    res += recoverable_scheduler->to_string();
    std::cout << recoverable_scheduler->to_string() << endl;
    delete recoverable_scheduler;

    dump_output_file(output_file_name, res);

    delete parser;
    return 0;
}