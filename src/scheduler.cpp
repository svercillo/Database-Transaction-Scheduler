#include "scheduler.h"

void Scheduler::populate_queue(vector<const Action *> actions){

    unordered_set<string> trans_ids;
    for (auto action : actions){
        ActionNode *node = new ActionNode(action);
        all_created_nodes.push_back(node);

        if (action->operation_type == OPERATIONTYPE_WRITE){
            if (transaction_writes.find(action->trans_id) == transaction_writes.end())
                transaction_writes.emplace(action->trans_id, unordered_set<string>{});

            transaction_writes[action->trans_id].insert(action->object_id);
        }

        if (trans_actions.find(action->trans_id) == trans_actions.end())
            trans_actions.emplace(action->trans_id, deque<ActionNode*>{});
        
        trans_actions[action->trans_id].push_back(node);

        trans_ids.insert(action->trans_id);
    }

    for (auto trans_id : trans_ids){
        queue.push(trans_actions[trans_id].front());
        trans_actions[trans_id].pop_front();
    }
}


void Scheduler::insert_node_into_schedule(ActionNode *node){

    string trans_id = node->action->trans_id;
    this->current_execution_time = max(node->action->time_offset, this->current_execution_time);
    node->exec_time = this->current_execution_time;
    this->current_execution_time += 1;

    this->nodes.push_back(node);
    
    if (trans_actions[trans_id].size() > 0){
        queue.push(trans_actions[trans_id].front());
        trans_actions[trans_id].pop_front();
    }
}

string Scheduler::to_string(){
    sort( // sort the actions
        nodes.begin(),
        nodes.end(),
        [](const ActionNode *a, const ActionNode *b)
        {
            return a->exec_time < b->exec_time; // when printing we want to return the nodes sorted on exect
        });

    string res = get_schedule_name() + "\n";

    for (auto node : nodes){
        res += node->to_string() + "\n";
    }
    
    if (this->deadlock_time != -1){
        res += "DEADLOCK DETECTED AT TIME " + std::to_string(this->deadlock_time) + "\n";
    }

    res += "\n";
    return res;
}


void Scheduler::print_queue(){
    cout << "QUEUE " << endl;
    vector<ActionNode *> arr;
    while (!queue.empty())
    {
        arr.push_back(queue.top());
        cout << queue.top()->to_string() << endl;
        queue.pop();
    }

    for (auto node : arr)
        queue.push(node);
        
    cout << endl;
}