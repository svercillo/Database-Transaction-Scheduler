#include "transactions_parser.h"

void TransactionsParser::fill_data_structures(){

    string contents = this->contents;

    istringstream iss(contents);
    string token;

    vector<string> tokens;
    WordType last_word_type = WORDTYPE_NULL;
    while (iss >> token)
    {
        std::transform(token.begin(), token.end(), token.begin(), [](unsigned char c) { return std::toupper(c); });
        tokens.push_back(token);
        switch (last_word_type)
        {
            case WORDTYPE_NULL:
            {
                last_word_type = WORDTYPE_TIMEOFFSET;
                break;
            }
            case WORDTYPE_TIMEOFFSET:
            {
                last_word_type = WORDTYPE_TRANSACTIONID;
                break;
            }
            case WORDTYPE_TRANSACTIONID:
            {
                int time_offset = stoi(tokens[0]);
                string trans_id = tokens[1];
                if (token == "C")
                {
                    const Action *action = new Action(time_offset, trans_id, OPERATIONTYPE_COMMIT);

                    this->actions_vec.push_back(action);
                    tokens.clear();
                    last_word_type = WORDTYPE_NULL;
                }
                else if (token == "A")
                {
                    const Action * action = new Action(time_offset, trans_id, OPERATIONTYPE_ABORT);

                    this->actions_vec.push_back(action);
                    tokens.clear();
                    last_word_type = WORDTYPE_NULL;
                }
                else if (token == "W")
                {
                    last_word_type = WORDTYPE_OPERATIONTYPE;
                }
                else if (token == "R")
                {
                    last_word_type = WORDTYPE_OPERATIONTYPE;
                }
                else if (token == "S")
                {
                    const Action * action = new Action(time_offset, trans_id, OPERATIONTYPE_START);
                    this->actions_vec.push_back(action);
                    tokens.clear();
                    last_word_type = WORDTYPE_NULL;
                }
                break;
            }
            case WORDTYPE_OPERATIONTYPE:
            {
                int time_offset = stoi(tokens[0]);
                string trans_id = tokens[1];
                string operation_str = tokens[2];
                string object_id = tokens[3];
                OperationType operation_type;
                if (operation_str == "W"){
                    operation_type = OPERATIONTYPE_WRITE;
                } else if (operation_str == "R"){
                    operation_type = OPERATIONTYPE_READ;
                }

                const Action * action = new Action(time_offset, trans_id, object_id, operation_type);
                this->actions_vec.push_back(action);
                tokens.clear();
                last_word_type = WORDTYPE_NULL;
                break;
            }
            default:
            {
                break;
            }
        }
    }
}