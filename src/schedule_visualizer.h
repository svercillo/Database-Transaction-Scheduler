#ifndef SCHEDULE_VISUALIZATION_H
#define SCHEDULE_VISUALIZATION_H

#include <vector>

#include "action.h"
#include "action_node.h"

using namespace std;

class ScheduleVisualizer
{ 
    public:
        static string visualize_schedule(vector<ActionNode*> nodes, bool cascadeless){

            string res = "";
            
            sort( // sort the actions
                nodes.begin(),
                nodes.end(),
                [](ActionNode *a, ActionNode *b)
                {
                    return *a < *b; // we can't compare the pointers directly, we have to compare the base objects
                });

            if (cascadeless)
                res += "Cascadeless Recoverable\n";
            else
                res += "Recoverable\n";

            
            
                string operation_string = get_operation_string(node->action->operation_type);

                if (
                    node->action->operation_type == OPERATIONTYPE_WRITE 
                    || node->action->operation_type == OPERATIONTYPE_READ
                ){
                    res += to_string(node->exec_time) + " " + to_string(node->action->time_offset) + " " + node->action->trans_id + " " + operation_string + " " + node->action->object_id;
                }
                else
                {
                    res += to_string(node->exec_time) + " " + to_string(node->action->time_offset) + " " + node->action->trans_id + " " + operation_string;
                }
            }
        }
};

#endif // SCHEDULE_VISUALIZATION_H