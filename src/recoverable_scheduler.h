#ifndef RECOVERABLE_SCHEULER_H
#define RECOVERABLE_SCHEULER_H

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <deque>


#include "scheduler.h"
#include "action.h"
#include "action_node.h"


using namespace std;

class RecoverableScheduler : public Scheduler
{ 
    public:
        RecoverableScheduler(vector<const Action *> actions){
            populate_queue(actions);
        }

        ~RecoverableScheduler(){
            for (auto action : dupliate_abort_actions_created){
                delete action;
            }
        }
        
        void schedule_tasks() override;

    private:
        void process_write(ActionNode *top_node) override;

        void process_read(ActionNode *top_node) override;

        void process_commit(ActionNode *top_node) override;

        void process_abort(ActionNode *top_node) override;
        
        void process_start(ActionNode *top_node) override;

        string get_schedule_name() override;

        string get_last_non_aborted_write_trans_id(string object_id);

        void move_waiting_nodes(string object_id, string trans_id);

        bool is_last_non_aborted_write_to_obj_committed(string object_id);

        bool is_trans_last_to_write_to_obj(string object_id, string trans_id);

        bool is_dirty_read(string object_id, string trans_id);

        void push_action_into_waiting(string trans_id, ActionNode *top_node);

        // for uncommited/unaborted actions, the key is object,
        // value is a list of the transactions in the order that they have written to this object
        unordered_map<string, vector<string>> obj_trans_have_written_to;


        unordered_map<string, unordered_set<string>> trans_that_are_waiting_for_objs_to_commit;

        unordered_map<string, unordered_set<string>> all_trans_ids_which_dirty_read_from_trans_id;

        // for a given object, all the actions that are waiting on this object to be committed (in any transaction)
        unordered_map<string, unordered_set<ActionNode*>> obj_nodes_are_waiting_on;

        vector<Action *> dupliate_abort_actions_created;
};

#endif // 