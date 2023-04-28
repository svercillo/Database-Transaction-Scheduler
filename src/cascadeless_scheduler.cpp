#include "cascadeless_scheduler.h"

void CascadelessScheduler::schedule_tasks(){ // can only occur after queue populated

    while (!queue.empty()){

        // print_queue();
        while(!queue.empty() && queue.top()->duplicate){
            queue.pop(); // pop any duplicates off
        }

        if (queue.empty()){ 
            // if queue is empty after popping all duplicates
            // cout << "Queue is empty" << endl;
            break;
        } 

        ActionNode *top_node = queue.top();
        queue.pop(); // pop node from queue

        // cout << "TOP NODE: " + top_node->to_string() << "   " << queue.size()  << (top_node->in_waiting_state ? " waitng" : "") << endl;
        
        switch(top_node->action->operation_type){
            case OPERATIONTYPE_WRITE:
            {
                process_write(top_node);
                break;
            }
            case OPERATIONTYPE_READ: // can have deadlock on reads
            {   
                if (top_node->in_waiting_state){
                    // cout << "DEADLOCK ON " << top_node->to_string() << endl;
                    this->deadlock_time = max(this->current_execution_time, top_node->action->time_offset);
                }
                process_read(top_node);
                break;
            }
            case OPERATIONTYPE_COMMIT: 
            {
                process_commit(top_node);
                break;
            }
            case OPERATIONTYPE_ABORT:
            {
                process_abort(top_node);
                break;
            }
            case OPERATIONTYPE_START:
            {
                process_start(top_node);
                break;
            }
            default:
                break;
        }
    }

    // cout << "queue.size() " << queue.size() << endl;
}

void CascadelessScheduler::process_write(ActionNode *top_node){
    if (this->deadlock_time != -1)
        return;

    string object_id = top_node->action->object_id;
    string trans_id = top_node->action->trans_id;
    // add the transaction to set of all transactions that have written to it's object
    if (obj_trans_have_written_to.find(object_id) == obj_trans_have_written_to.end())
        obj_trans_have_written_to.emplace(object_id, vector<string>{});
    
    obj_trans_have_written_to[object_id].push_back(trans_id);
    // cout << "Transaction " << trans_id << " writting to object " << object_id << endl;

    insert_node_into_schedule(top_node);
}

void CascadelessScheduler::process_read(ActionNode * top_node){
    string object_id = top_node->action->object_id;
    string trans_id = top_node->action->trans_id;

    if (this->deadlock_time != -1)
        return;
    
    if (is_dirty_read(object_id, trans_id))
    {
        // Check cycle
        string last_write_to_obj_trans_id = get_last_non_aborted_write_trans_id(object_id); // last write is uncommited

        // check if the object this action is reading from is uncommited write which is not from its own transaction
        if (trans_waiting_on_objs.find(trans_id) == trans_waiting_on_objs.end())
            trans_waiting_on_objs.emplace(trans_id, unordered_set<string>{});

        trans_waiting_on_objs[trans_id].insert(object_id); // add to the set of uncommited objects that are blocking this read

        if (obj_transactions_are_waiting_on.find(trans_id) == obj_transactions_are_waiting_on.end())
            obj_transactions_are_waiting_on.emplace(trans_id, unordered_set<string>{});

        obj_transactions_are_waiting_on[trans_id].insert(object_id);

        if (obj_nodes_are_waiting_on.find(object_id) == obj_nodes_are_waiting_on.end())
            obj_nodes_are_waiting_on.emplace(object_id, unordered_set<ActionNode *>{});
        
        obj_nodes_are_waiting_on[object_id].insert(top_node);

    }
    
    // check if transaction is blocked (read might be on B, but action is blocked cause of uncommited A)
    if (
        trans_waiting_on_objs.find(trans_id) != trans_waiting_on_objs.end() 
        && trans_waiting_on_objs[trans_id].size() >0
    ){
        top_node->in_waiting_state = true; // this node is now blocked
        queue.push(top_node); // push top_node back on the list
    } else { 
        // we can do read right now
        insert_node_into_schedule(top_node); 
    }
}

void CascadelessScheduler::process_commit(ActionNode * top_node){
    string trans_id = top_node->action->trans_id;
    
    if (this->deadlock_time != -1)
        return;

    // check if transaction is blocked on uncommitter write
    if (
        trans_waiting_on_objs.find(trans_id) != trans_waiting_on_objs.end() 
        && trans_waiting_on_objs[trans_id].size() > 0
    ){
        top_node->in_waiting_state = true;
        queue.push(top_node);
    } else {
        top_node->in_waiting_state = false; // no longer blocked

        for (auto object_id : transaction_writes[trans_id]){
            vector<string> write_history = obj_trans_have_written_to[object_id];

            // see if for any write, see if this transaction has the last write to this object
            if (is_trans_last_to_write_to_obj(object_id, trans_id)){
                // cout << "Committing last write" << endl;
                // if committing the last write to an object, remove waiting nodes
                move_waiting_reads(object_id, trans_id);
            }
        }
    
        // commit transaction
        committed_transactions.insert(trans_id);
        insert_node_into_schedule(top_node);
    }
}

void CascadelessScheduler::process_abort(ActionNode *top_node){
    string trans_id = top_node->action->trans_id;

    if (this->deadlock_time != -1)
        return;
    
    // check if transaction is blocked on uncommitter write
    if (
        trans_waiting_on_objs.find(trans_id) != trans_waiting_on_objs.end() 
        && trans_waiting_on_objs[trans_id].size() > 0
    ){
        top_node->in_waiting_state = true;
        queue.push(top_node);

    } else {
        // abort transaction before checking if last write to objects are committed
        aborted_transactions.insert(trans_id);

        for (auto object_id : transaction_writes[trans_id]){

            vector<string> write_history = obj_trans_have_written_to[object_id];
            // cout << "OBJECT " << object_id << endl;
  
            if (is_last_non_aborted_write_to_obj_committed(object_id))
                move_waiting_reads(object_id, trans_id);
        }
        
        insert_node_into_schedule(top_node);
    }
}

void CascadelessScheduler::process_start(ActionNode *top_node){
    insert_node_into_schedule(top_node);
}

void CascadelessScheduler::move_waiting_reads(string object_id, string trans_id){

    // if (trans_id == "T2")
    // cout << "WAITING " << endl;
    for (auto action_node : obj_nodes_are_waiting_on[object_id])
    {
        // if (trans_id == "T2")
        // cout << action_node->to_string() << endl;
        // cout << action_node->to_string() << endl;
        // the transactions waiting on this are no longer waiting (i.e. if read on object_id was blocked, it is no longer)
        if (trans_waiting_on_objs.find(action_node->action->trans_id) != trans_waiting_on_objs.end()){
            if (
                trans_waiting_on_objs[action_node->action->trans_id].find(object_id) 
                != trans_waiting_on_objs[action_node->action->trans_id].end()
            )
                trans_waiting_on_objs[action_node->action->trans_id].erase(object_id); // transaction is no longer waiting on this object
        }

        if (!action_node->duplicate){
            action_node->duplicate = true; // set current node to duplicate
            ActionNode *copy_node = new ActionNode(action_node);
            all_created_nodes.push_back(copy_node);
            queue.push(copy_node); // action is no longer waiting
        }

        // trans_id is no longer waiting (its being committed)
        if (trans_waiting_on_objs.find(trans_id) != trans_waiting_on_objs.end()){
            if (trans_waiting_on_objs[trans_id].find(object_id) != trans_waiting_on_objs[trans_id].end())
                trans_waiting_on_objs[trans_id].erase(object_id); // transaction is no longer waiting on this object
        }
    }
}

string CascadelessScheduler::get_last_non_aborted_write_trans_id(string object_id){
    if (
        obj_trans_have_written_to.find(object_id) == obj_trans_have_written_to.end()
        || obj_trans_have_written_to[object_id].size() == 0
    )
        return ""; // no writes ever to object. Assumption is that whatever is in DB is committed

    vector<string> write_history = obj_trans_have_written_to[object_id];

    int history_len = write_history.size();
    int _p = history_len - 1;

    vector<int> inds_to_remove;

    while (_p >= 0 && aborted_transactions.find(write_history[_p]) != aborted_transactions.end()){
        inds_to_remove.push_back(_p);
        _p--;
    }

    // // pop any writes off that were from aborted commits
    // for (int i = inds_to_remove.size() - 1; i >= 0; i--){
    //     write_history.erase(write_history.begin() + inds_to_remove[i]);
    // }

    if (_p < 0)
        return ""; // all writes were from transactions that were aborted.

    string last_written_trans_id = write_history[_p]; // trans_id of the last write to this object

    return last_written_trans_id;

}
        
bool CascadelessScheduler::is_last_non_aborted_write_to_obj_committed(string object_id){
    // cout << "HERHE" << endl;
    string last_non_aborted_write_trans_id = get_last_non_aborted_write_trans_id(object_id);

    if (last_non_aborted_write_trans_id == "")
        return true; // no writes remain
    else if (committed_transactions.find(last_non_aborted_write_trans_id) != committed_transactions.end())
        return true; // last write is committed
    else
        return false;
}

bool CascadelessScheduler::is_trans_last_to_write_to_obj(string object_id, string trans_id){

    string last_non_aborted_write_trans_id = get_last_non_aborted_write_trans_id(object_id);

    if (last_non_aborted_write_trans_id == trans_id)
        return true; // trans_id is the transaction which last wrote to this object
    else 
        return false;
}

bool CascadelessScheduler::is_dirty_read(string object_id, string trans_id){

    string last_non_aborted_write_trans_id = get_last_non_aborted_write_trans_id(object_id);

    if (last_non_aborted_write_trans_id == "")
        return false; // nothing is persisted on object_id
    else if (last_non_aborted_write_trans_id == trans_id)
        return false; // last write was from this transaction
    else if (committed_transactions.find(last_non_aborted_write_trans_id) != committed_transactions.end())
        return false; // last write was from a committed transaction
    else
        return true;
}

string CascadelessScheduler::get_schedule_name(){
    return "Cascadeless Recoverable";
}