
#ifndef SERIAL_SCHEDULER_H
#define SERIAL_SCHEDULER_H

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>


#include "scheduler.h"
#include "action.h"
#include "action_node.h"


using namespace std;

class SerialScheduler : public Scheduler
{ 
    public:
        SerialScheduler(vector<const Action *> actions){
            populate_queue(actions);
        }

        void schedule_tasks() override;

    private:
        void process_write(ActionNode *top_node) override {} // not used

        void process_read(ActionNode *top_node) override {}; // not used

        void process_commit(ActionNode *top_node) override {}; // not used

        void process_abort(ActionNode *top_node) override {}; // not used
        
        void process_start(ActionNode *top_node) override {}; // not used

        string get_schedule_name() override;

        void move_waiting_reads(string object_id, string trans_id);

        string get_last_non_aborted_write_trans_id(string object_id);

        bool is_last_non_aborted_write_to_obj_committed(string object_id);

        bool is_trans_last_to_write_to_obj(string object_id, string trans_id);

        bool is_dirty_read(string object_id, string trans_id);
        
        bool compare_transactions(const std::string& s1, const std::string& s2);

        unordered_map<string, vector<ActionNode*>> actions_per_transaction;
};

#endif // SERIAL_SCHEDULER_H