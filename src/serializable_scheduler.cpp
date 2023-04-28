#include "serializable_scheduler.h"

void SerializableScheduler::schedule_tasks(){ // can only occur after queue populated0

    while (!queue.empty() && this->deadlock_time == -1){
        while(!queue.empty() && queue.top()->duplicate){
            queue.pop(); // pop any duplicates off
        }

        if (queue.empty()){ 
            // if queue is empty after popping all duplicates
            break;
        }

        ActionNode *top_node = queue.top();
        queue.pop(); // pop node from queue
        
        switch(top_node->action->operation_type){
            case OPERATIONTYPE_WRITE:
            {
                if (top_node->in_waiting_state){
                    this->deadlock_time = max(this->current_execution_time, top_node->action->time_offset);
                }
                process_write(top_node);
                break;
            }
            case OPERATIONTYPE_READ: // can have deadlock on reads
            {   
                if (top_node->in_waiting_state){
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
}

void SerializableScheduler::process_write(ActionNode *top_node){

    if (this->deadlock_time != -1)
        return;

    string trans_id = top_node->action->trans_id;
    string object_id = top_node->action->object_id;

    if (
        obj_locks.find(object_id) != obj_locks.end()
        && (// if theres a lock, its not owned by this transaction
            locks_held_by_trans_id.find(trans_id) == locks_held_by_trans_id.end()
            || locks_held_by_trans_id[trans_id].find(object_id) == locks_held_by_trans_id[trans_id].end()
        )
    ){ // if object is already being used
        if (objs_trans_are_waiting_on.find(object_id) == objs_trans_are_waiting_on.end()){}
            objs_trans_are_waiting_on.emplace(object_id, unordered_set<string>{});
        objs_trans_are_waiting_on[trans_id].insert(object_id);
        
        if (obj_nodes_are_waiting_on.find(object_id) == obj_nodes_are_waiting_on.end()){}
            obj_nodes_are_waiting_on.emplace(object_id, unordered_set<ActionNode*>{});

        obj_nodes_are_waiting_on[object_id].insert(top_node);
        
        top_node->in_waiting_state = true;
        queue.push(top_node);
    } else {
        obj_locks.insert(object_id);

        if (locks_held_by_trans_id.find(trans_id) == locks_held_by_trans_id.end())
            locks_held_by_trans_id.emplace(trans_id, unordered_set<string>{});

        locks_held_by_trans_id[trans_id].insert(object_id);
        insert_node_into_schedule(top_node);
    }
}

void SerializableScheduler::process_read(ActionNode * top_node){
    process_write(top_node);
}

void SerializableScheduler::process_commit(ActionNode * top_node){
    if (this->deadlock_time != -1)
        return;
    
    string trans_id = top_node->action->trans_id;
    objs_trans_are_waiting_on.find(trans_id) != objs_trans_are_waiting_on.end();

    
    
    if (
        objs_trans_are_waiting_on.find(trans_id) != objs_trans_are_waiting_on.end()
        && objs_trans_are_waiting_on[trans_id].size() > 0
    ){ // if object is already being used

        for (auto object_id : objs_trans_are_waiting_on[trans_id]){

            if (obj_nodes_are_waiting_on.find(object_id) == obj_nodes_are_waiting_on.end()){}
                obj_nodes_are_waiting_on.emplace(object_id, unordered_set<ActionNode*>{});
            obj_nodes_are_waiting_on[object_id].insert(top_node);
        }
        top_node->in_waiting_state = true;
        queue.push(top_node);

    } else {
        // release all locks that trans held
        if (locks_held_by_trans_id.find(trans_id) != locks_held_by_trans_id.end()){
            for (auto object_id : locks_held_by_trans_id[trans_id]){

                // for all nodes waiting on object, remove from waiting
                if (obj_nodes_are_waiting_on.find(object_id) != obj_nodes_are_waiting_on.end()){
                    for (auto node: obj_nodes_are_waiting_on[object_id]){

                        string dep_trans_id = node->action->trans_id;

                        if (objs_trans_are_waiting_on.find(dep_trans_id) != objs_trans_are_waiting_on.end()){
                            if (objs_trans_are_waiting_on[dep_trans_id].find(object_id) != objs_trans_are_waiting_on[dep_trans_id].end()){
                                objs_trans_are_waiting_on[dep_trans_id].erase(object_id);
                            }
                        }
                        
                        if (!node->duplicate){
                            ActionNode * copy_node = new ActionNode(node);
                            node->duplicate = true;
                            all_created_nodes.push_back(copy_node);
                            queue.push(copy_node);
                        }
                        
                    }
                }

                obj_locks.erase(object_id);
            }
        }

        insert_node_into_schedule(top_node);
    }    
}

void SerializableScheduler::process_abort(ActionNode * top_node){
    process_commit(top_node);
}


void SerializableScheduler::process_start(ActionNode *top_node){
    insert_node_into_schedule(top_node);
}


string SerializableScheduler::get_schedule_name(){
    return "Serializable Scheduler";
}
