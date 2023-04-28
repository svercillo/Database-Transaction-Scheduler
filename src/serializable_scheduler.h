
#ifndef serial_scheduler_H
#define serial_scheduler_H

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>


#include "scheduler.h"
#include "action.h"
#include "action_node.h"


using namespace std;

class SerializableScheduler : public Scheduler
{ 
    public:
        SerializableScheduler(vector<const Action *> actions){
            populate_queue(actions);
        }

        void schedule_tasks() override;

    private:
        void process_write(ActionNode *top_node) override;

        void process_read(ActionNode *top_node) override;

        void process_commit(ActionNode *top_node) override;

        void process_abort(ActionNode *top_node) override;
        
        void process_start(ActionNode *top_node) override;

        string get_schedule_name() override;

        void move_waiting_reads(string object_id, string trans_id);

        string get_last_non_aborted_write_trans_id(string object_id);

        bool is_last_non_aborted_write_to_obj_committed(string object_id);

        bool is_trans_last_to_write_to_obj(string object_id, string trans_id);

        bool is_dirty_read(string object_id, string trans_id);
        
        void push_node(ActionNode * node);

        unordered_map<string, vector<ActionNode*>> actions_per_transaction;

        unordered_set<string> obj_locks;

        unordered_map<string, unordered_set<string>> locks_held_by_trans_id;

        // for a given object, all the actions that are waiting on this object to be committed (in any transaction)
        unordered_map<string, unordered_set<ActionNode*>> obj_nodes_are_waiting_on;

        unordered_map<string, unordered_set<string>> objs_trans_are_waiting_on;
};

#endif // serial_scheduler_H