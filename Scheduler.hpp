//
//  Scheduler.hpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <array>
#include <cstdint>
#include <deque>
#include <vector>

#include "Interfaces.h"

class Scheduler {
public:
    Scheduler()                 {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);
private:
    Priority_t GetPriorityForSLA(SLAType_t sla) const;
    VMId_t SelectVMForTask(TaskId_t task_id) const;
    void DispatchPendingTasks();

    vector<VMId_t> vms;
    vector<MachineId_t> machines;
    array<deque<TaskId_t>, PRIORITY_LEVELS> pending_tasks;
};



#endif /* Scheduler_hpp */
