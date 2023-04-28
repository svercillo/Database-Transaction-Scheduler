#ifndef ACTIONNODE_H
#define ACTIONNODE_H

#include <unordered_map>
#include <iostream>
#include <vector>

#include "action.h"
using namespace std;


struct ActionNode
{ 
    public:
        const Action * action; // should be an immutable reference
        int exec_time = -1;
        bool in_waiting_state = false;
        bool in_waiting_for_par_state = false;
        bool duplicate = false;

        bool operator>(const ActionNode& other) const {
            if (this->in_waiting_state == other.in_waiting_state){
                return this->action->time_offset > other.action->time_offset;
            } else{
                return this->in_waiting_state; // the node in the waiting state should be ahead of the other node
            }
        }

        ActionNode(){}
        
        ActionNode(const Action* action){
            this->action = action;
        }

        ActionNode(ActionNode* other){
            this->action = other->action;
        }

        ~ActionNode(){
            // don't deallocate action
        }

        string to_string() const {
            string res = "";
            string operation_string = get_operation_string(action->operation_type);

            if (action->operation_type == OPERATIONTYPE_WRITE || action->operation_type == OPERATIONTYPE_READ){
                res += std::to_string(exec_time) + " " + std::to_string(action->time_offset) + " " + action->trans_id + " " + operation_string + " " + action->object_id + (in_waiting_state ? " waitng" : "") + (duplicate ? " duplciate" : "");
            }
            else
            {
                res += std::to_string(exec_time) + " " + std::to_string(action->time_offset) + " " + action->trans_id + " " + operation_string + (in_waiting_state ? " waitng" : "") + (duplicate ? " duplciate" : "");
            }
            return res;
        };

        string get_operation_string(OperationType operation_type) const {
            switch (operation_type)
            {
                case OPERATIONTYPE_WRITE:
                    return  "W";
                case OPERATIONTYPE_READ:
                    return "R";
                case OPERATIONTYPE_COMMIT:
                    return "C";
                case OPERATIONTYPE_ABORT:
                    return "A";
                case OPERATIONTYPE_START:
                    return "S";
                default:
                    return "unknown operation";
            }
        }
};

class ActionNodeComparator{ 
    public: 
        bool operator()(const ActionNode* obj1, const ActionNode* obj2) const {
            return *obj1 > *obj2;
        }
};

#endif //ACTIONNODE_H