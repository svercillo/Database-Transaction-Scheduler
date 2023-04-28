#include "recoverable_scheduler.h"


void RecoverableScheduler::schedule_tasks(){ // can only occur after queue populated

    while (!queue.empty() && this->deadlock_time == -1){
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
        
        switch(top_node->action->operation_type){
            case OPERATIONTYPE_WRITE:
            {
                process_write(top_node);
                break;
            }
            case OPERATIONTYPE_READ: // can have deadlock on reads
            {   
                process_read(top_node);
                break;
            }
            case OPERATIONTYPE_COMMIT: 
            {
                if (top_node->in_waiting_state){
                    this->deadlock_time = max(this->current_execution_time, top_node->action->time_offset);
                }
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

void RecoverableScheduler::process_write(ActionNode *top_node){
    string object_id = top_node->action->object_id;
    string trans_id = top_node->action->trans_id;

    if (
        aborted_transactions.find(top_node->action->trans_id) != aborted_transactions.end()
        || this->deadlock_time != -1 // meaning deadlock occured
    ){
        return;
    }

    // add the transaction to set of all transactions that have written to it's object
    if (obj_trans_have_written_to.find(object_id) == obj_trans_have_written_to.end())
        obj_trans_have_written_to.emplace(object_id, vector<string>{});
    
    obj_trans_have_written_to[object_id].push_back(trans_id);

    insert_node_into_schedule(top_node);
}

void RecoverableScheduler::process_read(ActionNode * top_node){
    string object_id = top_node->action->object_id;
    string trans_id = top_node->action->trans_id;


    if (
        aborted_transactions.find(top_node->action->trans_id) != aborted_transactions.end()
        || this->deadlock_time != -1 // meaning deadlock occured
    ){
        return;
    }

    string last_non_aborted_write_trans_id = get_last_non_aborted_write_trans_id(object_id);

    if (
        last_non_aborted_write_trans_id != ""
        && committed_transactions.find(last_non_aborted_write_trans_id) == committed_transactions.end()
        && last_non_aborted_write_trans_id != trans_id
    ){ // dirty read
        
        // cout << "DIRTY READ: " << top_node->to_string() << endl;
        // dirty reads are okay, but we need to track them
    
        if (trans_that_are_waiting_for_objs_to_commit.find(trans_id) == trans_that_are_waiting_for_objs_to_commit.end())
            trans_that_are_waiting_for_objs_to_commit.emplace(trans_id, unordered_set<string>{});

        if (all_trans_ids_which_dirty_read_from_trans_id.find(trans_id) == all_trans_ids_which_dirty_read_from_trans_id.end())
            all_trans_ids_which_dirty_read_from_trans_id.emplace(trans_id, unordered_set<string>{});

        // indicates that trans_id must wait until object becomes committed (last non-abort write must not exist or be committed)
        trans_that_are_waiting_for_objs_to_commit[trans_id].insert(object_id);

        // last_non_aborted_write_trans_id is read dirtily from trans_id  
        all_trans_ids_which_dirty_read_from_trans_id[last_non_aborted_write_trans_id].insert(trans_id); 
    }

    // we can do read right now
    insert_node_into_schedule(top_node); 
}

void RecoverableScheduler::process_commit(ActionNode * top_node){
    string trans_id = top_node->action->trans_id;
    
    if (
        aborted_transactions.find(top_node->action->trans_id) != aborted_transactions.end()
        || this->deadlock_time != -1 // meaning deadlock occured
    ){
        return;
    }

    // check if transaction is blocked on uncommitted write
    if (
        trans_that_are_waiting_for_objs_to_commit.find(trans_id) != trans_that_are_waiting_for_objs_to_commit.end() 
        && trans_that_are_waiting_for_objs_to_commit[trans_id].size() > 0
    ){
        push_action_into_waiting(trans_id, top_node); 
    } else {
        top_node->in_waiting_state = false; // no longer blocked
    
        for (auto object_id : transaction_writes[trans_id]){
            vector<string> write_history = obj_trans_have_written_to[object_id];

            // see if for any write, see if this transaction has the last write to this object
            if (is_trans_last_to_write_to_obj(object_id, trans_id)){
                // if committing the last write to an object, remove waiting nodes
                move_waiting_nodes(object_id, trans_id);
            }
        }
        // commit transaction
        committed_transactions.insert(trans_id);

        // cout << "Processing commit " << top_node << endl;
        insert_node_into_schedule(top_node);
    }
}

void RecoverableScheduler::process_abort(ActionNode * top_node){
 
    deque<ActionNode*> q;

    if (
        aborted_transactions.find(top_node->action->trans_id) != aborted_transactions.end()
        || this->deadlock_time != -1 // meaning deadlock occured
    ){
        return;
    }
    
    q.push_back(top_node);
    bool first_iter = true;

    while (q.size() > 0 ){
        top_node = q.front();
        insert_node_into_schedule(top_node);

        string trans_id = top_node->action->trans_id;;
        q.pop_front();

        if (
            committed_transactions.find(trans_id) != committed_transactions.end() 
            || aborted_transactions.find(trans_id) != aborted_transactions.end()
        )
            continue;

        // aborts can happen instantly
        aborted_transactions.insert(trans_id);

        if (all_trans_ids_which_dirty_read_from_trans_id.find(trans_id) == all_trans_ids_which_dirty_read_from_trans_id.end())
            continue; // no dirty reads from this transaction

        // abort all the transactions dirty read from this transaction
        for (auto dep_trans_id : all_trans_ids_which_dirty_read_from_trans_id[trans_id]){

            // cout << "DEP TRANS ID " << dep_trans_id << endl;
            // create a new abort
            Action *abort_action = new Action(this->current_execution_time, dep_trans_id, OPERATIONTYPE_ABORT); // TODO DELETE THIS POINTER
            ActionNode * abort_node = new ActionNode(abort_action); // TODO DELETE THIS POINTER
            this->all_created_nodes.push_back(abort_node);

            dupliate_abort_actions_created.push_back(abort_action);
            q.push_back(abort_node);
        }
    }
}


void RecoverableScheduler::process_start(ActionNode *top_node){
    insert_node_into_schedule(top_node);
}


void RecoverableScheduler::push_action_into_waiting(string trans_id, ActionNode * top_node){
    for (auto object_id : trans_that_are_waiting_for_objs_to_commit[trans_id]){
        if (obj_nodes_are_waiting_on.find(object_id) == obj_nodes_are_waiting_on.end())
            obj_nodes_are_waiting_on.emplace(object_id, unordered_set<ActionNode *>{});
        
        obj_nodes_are_waiting_on[object_id].insert(top_node);
    }

    top_node->in_waiting_state = true;
    queue.push(top_node);
}



void RecoverableScheduler::move_waiting_nodes(string object_id, string trans_id){
    // object has committed so, so any transactions that were waiting for this obj can be moved from waiting
    for (auto action_node : obj_nodes_are_waiting_on[object_id])
    {   
        // the transactions waiting on this are no longer waiting (i.e. if read on object_id was blocked, it is no longer)
        if (trans_that_are_waiting_for_objs_to_commit.find(action_node->action->trans_id) != trans_that_are_waiting_for_objs_to_commit.end()){
            if (
                trans_that_are_waiting_for_objs_to_commit[action_node->action->trans_id].find(object_id) 
                != trans_that_are_waiting_for_objs_to_commit[action_node->action->trans_id].end()
            )
                // transaction is no longer waiting on this object
                trans_that_are_waiting_for_objs_to_commit[action_node->action->trans_id].erase(object_id);
        }

        if (!action_node->duplicate){
            action_node->duplicate = true; // set current node to duplicate
            ActionNode *copy_node = new ActionNode(action_node);
            all_created_nodes.push_back(copy_node);
            queue.push(copy_node); // action is no longer waiting
        }
        
        // trans_id is no longer waiting (its being committed)
        if (trans_that_are_waiting_for_objs_to_commit.find(trans_id) != trans_that_are_waiting_for_objs_to_commit.end()){
            if (trans_that_are_waiting_for_objs_to_commit[trans_id].find(object_id) != trans_that_are_waiting_for_objs_to_commit[trans_id].end())
                trans_that_are_waiting_for_objs_to_commit[trans_id].erase(object_id); // transaction is no longer waiting on this object
        }
    }
}

string RecoverableScheduler::get_last_non_aborted_write_trans_id(string object_id){
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
        
bool RecoverableScheduler::is_last_non_aborted_write_to_obj_committed(string object_id){
    string last_non_aborted_write_trans_id = get_last_non_aborted_write_trans_id(object_id);

    if (last_non_aborted_write_trans_id == "")
        return true; // no writes remain
    else if (committed_transactions.find(last_non_aborted_write_trans_id) != committed_transactions.end())
        return true; // last write is committed
    else
        return false;
}

bool RecoverableScheduler::is_trans_last_to_write_to_obj(string object_id, string trans_id){

    string last_non_aborted_write_trans_id = get_last_non_aborted_write_trans_id(object_id);

    if (last_non_aborted_write_trans_id == trans_id)
        return true; // trans_id is the transaction which last wrote to this object
    else 
        return false;
}

bool RecoverableScheduler::is_dirty_read(string object_id, string trans_id){

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

string RecoverableScheduler::get_schedule_name(){
    return "Recoverable";
}