#ifndef TRANSACTIONS_PARSER_H
#define TRANSACTIONS_PARSER_H

#include "action.h"

#include <sstream>
#include <algorithm>
using namespace std;

enum WordType
{
    WORDTYPE_TIMEOFFSET,
    WORDTYPE_TRANSACTIONID,
    WORDTYPE_OPERATIONTYPE,
    WORDTYPE_OBJECTID,
    WORDTYPE_NULL
};

class TransactionsParser{ 
    public:
        vector<const Action*> actions_vec;
        
        TransactionsParser(std::string contents)
        {
            this->contents = contents;
            fill_data_structures();
        }

        ~TransactionsParser(){
            for (auto action : actions_vec){
                delete action;
            }
        }
    private:
        string contents;
        
        void fill_data_structures();
};

#endif //TRANSACTIONS_PARSER_H