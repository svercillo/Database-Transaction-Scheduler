#ifndef CASCADELESS_SCHEDULER_H
#define CASCADELESS_SCHEDULER_H

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>


#include "scheduler.h"
#include "action.h"
#include "action_node.h"


using namespace std;

class CascadelessScheduler : public Scheduler
{ 
    public:
        CascadelessScheduler(vector<const Action *> actions){
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

        // for uncommited/unaborted actions, the key is object,
        // value is a list of the transactions in the order that they have written to this object
        unordered_map<string, vector<string>> obj_trans_have_written_to;

        // for a given object, all the actions that are waiting on this object to be committed (in any transaction)
        unordered_map<string, unordered_set<ActionNode*>> obj_nodes_are_waiting_on;

        // for a given object, all the transactions that are waiting on this object to have a write commited (or abort until a committed write)
        unordered_map<string, unordered_set<string>> obj_transactions_are_waiting_on;

        // for a given transaction, all the objects that this transaction is waiting to be commited
        unordered_map<string, unordered_set<string>> trans_waiting_on_objs;

        // the object a transcation
        unordered_map<string, string> trans_blocked_on; 
};

#endif // CASCADELESS_SCHEDULER_H