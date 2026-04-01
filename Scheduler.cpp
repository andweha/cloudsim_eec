//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <cstdlib>
#include <limits>

static bool migrating = false;
static unsigned active_machines = 16;
static constexpr VMId_t INVALID_VM = std::numeric_limits<VMId_t>::max();

void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    vms.clear();
    machines.clear();
    for(auto &queue : pending_tasks) queue.clear();

    const unsigned total_machines = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(total_machines), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    for(unsigned i = 0; i < total_machines; i++) {
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        if(info.s_state != S0) continue;

        VMId_t vm_id = VM_Create(LINUX, info.cpu);
        VM_Attach(vm_id, MachineId_t(i));
        machines.push_back(MachineId_t(i));
        vms.push_back(vm_id);
    }
    active_machines = static_cast<unsigned>(machines.size());

    const char *algo_env = std::getenv("SCHED_ALGO");
    string algo = (algo_env == nullptr) ? "greedy" : string(algo_env);
    if(algo == "greedy") algorithm = Algorithm_t::GREEDY;
    else if(algo == "pmapper") algorithm = Algorithm_t::PMAPPER;
    else if(algo == "round_robin") algorithm = Algorithm_t::ROUND_ROBIN;
    else if(algo == "e_eco") algorithm = Algorithm_t::E_ECO;
    else {
        algorithm = Algorithm_t::GREEDY;
        SimOutput("Scheduler::Init(): Unknown SCHED_ALGO=" + algo + ", defaulting to greedy", 1);
    }

    bool dynamic = false;
    if(dynamic)
        for(unsigned i = 0; i<4 ; i++)
            for(unsigned j = 0; j < 8; j++)
                Machine_SetCorePerformance(MachineId_t(0), j, P3);
    if(vms.empty()) {
        ThrowException("Scheduler::Init(): No machines found to attach VMs");
    }
    SimOutput("Scheduler::Init(): Initialized " + to_string(vms.size()) + " VM(s)", 3);
    SimOutput("Scheduler::Init(): Active algorithm is " + algo, 2);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    (void)time;
    (void)vm_id;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    //  IsGPUCapable(task_id);
    //  GetMemory(task_id);
    //  RequiredVMType(task_id);
    //  RequiredSLA(task_id);
    //  RequiredCPUType(task_id);
    // Decide to attach the task to an existing VM, 
    //      vm.AddTask(taskid, Priority_T priority); or
    // Create a new VM, attach the VM to a machine
    //      VM vm(type of the VM)
    //      vm.Attach(machine_id);
    //      vm.AddTask(taskid, Priority_t priority) or
    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired
    switch(algorithm) {
        case Algorithm_t::GREEDY:
            NewTaskGreedy(now, task_id);
            break;
        case Algorithm_t::PMAPPER:
            NewTaskPMapper(now, task_id);
            break;
        case Algorithm_t::ROUND_ROBIN:
            NewTaskRoundRobin(now, task_id);
            break;
        case Algorithm_t::E_ECO:
            NewTaskEEco(now, task_id);
            break;
        default:
            NewTaskGreedy(now, task_id);
            break;
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
    (void)now;
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
}

void Scheduler::NewTaskGreedy(Time_t now, TaskId_t task_id) {
    (void)now;
    Priority_t priority = GetPriorityForSLA(RequiredSLA(task_id));
    if(!TryPlaceTask(task_id, priority))
        SimOutput("Scheduler::NewTaskGreedy(): Could not place task " + to_string(task_id), 1);
}

void Scheduler::NewTaskPMapper(Time_t now, TaskId_t task_id) {
    (void)now;
    Priority_t priority = GetPriorityForSLA(RequiredSLA(task_id));
    if(!TryPlaceTask(task_id, priority))
        SimOutput("Scheduler::NewTaskPMapper(): Could not place task " + to_string(task_id), 1);
}

void Scheduler::NewTaskRoundRobin(Time_t now, TaskId_t task_id) {
    (void)now;
    Priority_t priority = GetPriorityForSLA(RequiredSLA(task_id));
    if(!TryPlaceTask(task_id, priority))
        SimOutput("Scheduler::NewTaskRoundRobin(): Could not place task " + to_string(task_id), 1);
}

void Scheduler::NewTaskEEco(Time_t now, TaskId_t task_id) {
    (void)now;
    Priority_t priority = GetPriorityForSLA(RequiredSLA(task_id));
    if(!TryPlaceTask(task_id, priority))
        SimOutput("Scheduler::NewTaskEEco(): Could not place task " + to_string(task_id), 1);
}

bool Scheduler::TryPlaceTask(TaskId_t task_id, Priority_t priority) {
    VMId_t target_vm = INVALID_VM;
    switch(algorithm) {
        case Algorithm_t::GREEDY:
            target_vm = SelectVMForTask(task_id);
            break;
        case Algorithm_t::PMAPPER:
            target_vm = SelectVMPMapper(task_id);
            break;
        case Algorithm_t::ROUND_ROBIN:
            target_vm = SelectVMRoundRobin(task_id);
            break;
        case Algorithm_t::E_ECO:
            target_vm = SelectVMEEco(task_id);
            break;
        default:
            target_vm = SelectVMForTask(task_id);
            break;
    }

    if(target_vm == INVALID_VM) return false;

    if(algorithm == Algorithm_t::E_ECO) {
        VMInfo_t vm_info = VM_GetInfo(target_vm);
        MachineId_t machine_id = vm_info.machine_id;
        CPUPerformance_t pstate = P1;
        switch(RequiredSLA(task_id)) {
            case SLA0: pstate = P0; break;
            case SLA1: pstate = P1; break;
            case SLA2: pstate = P1; break;
            case SLA3: pstate = P2; break;
            default: pstate = P1; break;
        }
        Machine_SetCorePerformance(machine_id, 0, pstate);
    }

    VM_AddTask(target_vm, task_id, priority);
    return true;
}

bool Scheduler::CanHostTask(VMId_t vm_id, TaskId_t task_id) const {
    VMInfo_t vm_info = VM_GetInfo(vm_id);
    if(vm_info.cpu != RequiredCPUType(task_id)) return false;
    if(vm_info.vm_type != RequiredVMType(task_id)) return false;

    MachineInfo_t machine_info = Machine_GetInfo(vm_info.machine_id);
    if(machine_info.s_state != S0) return false;
    return true;
}

Priority_t Scheduler::GetPriorityForSLA(SLAType_t sla) const {
    switch(sla) {
        case SLA0:
            return HIGH_PRIORITY;
        case SLA1:
            return MID_PRIORITY;
        case SLA2:
        case SLA3:
            return LOW_PRIORITY;
        default:
            return MID_PRIORITY;
    }
}

void Scheduler::DispatchPendingTasks() {
    // Dispatch in strict priority order: high, then mid, then low.
    // If a task cannot be placed now (resource pressure), keep it queued.
    for(int p = HIGH_PRIORITY; p <= LOW_PRIORITY; p++) {
        auto &queue = pending_tasks[p];
        const size_t pending_count = queue.size();
        for(size_t i = 0; i < pending_count; i++) {
            TaskId_t next_task = queue.front();
            queue.pop_front();
            if(!TryPlaceTask(next_task, static_cast<Priority_t>(p))) {
                queue.push_back(next_task);
            }
        }
    }
}

VMId_t Scheduler::SelectVMForTask(TaskId_t task_id) const {
    const CPUType_t required_cpu = RequiredCPUType(task_id);
    const VMType_t required_vm = RequiredVMType(task_id);
    const unsigned required_mem = GetTaskMemory(task_id);
    const SLAType_t required_sla = RequiredSLA(task_id);
    const bool task_gpu_capable = IsTaskGPUCapable(task_id);
    const bool latency_sensitive = (required_sla == SLA0 || required_sla == SLA1);

    VMId_t best_vm = vms[0];
    bool found = false;
    unsigned best_score = 0;
    bool has_gpu_candidate = false;
    bool has_nongpu_candidate = false;

    for(const auto vm_id : vms) {
        VMInfo_t vm_info = VM_GetInfo(vm_id);
        if(vm_info.cpu != required_cpu) continue;
        if(vm_info.vm_type != required_vm) continue;

        MachineInfo_t machine_info = Machine_GetInfo(vm_info.machine_id);
        if(machine_info.s_state != S0) continue;
        (void)required_mem;

        if(machine_info.gpus) has_gpu_candidate = true;
        else has_nongpu_candidate = true;
    }

    for(const auto vm_id : vms) {
        VMInfo_t vm_info = VM_GetInfo(vm_id);
        if(vm_info.cpu != required_cpu) continue;
        if(vm_info.vm_type != required_vm) continue;

        MachineInfo_t machine_info = Machine_GetInfo(vm_info.machine_id);
        if(machine_info.s_state != S0) continue;
        if(task_gpu_capable && has_gpu_candidate && !machine_info.gpus) continue;
        if(!task_gpu_capable && has_nongpu_candidate && machine_info.gpus) continue;

        // Greedy policy:
        // - For tighter SLAs, spread load to reduce queueing delay.
        // - For loose SLAs, consolidate to save energy.
        unsigned score = machine_info.active_tasks;
        if(!found) {
            found = true;
            best_score = score;
            best_vm = vm_id;
            continue;
        }

        if(latency_sensitive && score < best_score) {
            best_score = score;
            best_vm = vm_id;
        }
        if(!latency_sensitive && score > best_score) {
            best_score = score;
            best_vm = vm_id;
        }
    }

    if(found) return best_vm;
    return INVALID_VM;
}

VMId_t Scheduler::SelectVMRoundRobin(TaskId_t task_id) {
    const unsigned vm_count = static_cast<unsigned>(vms.size());
    const bool task_gpu_capable = IsTaskGPUCapable(task_id);
    if(vm_count == 0) {
        ThrowException("Scheduler::SelectVMRoundRobin(): No VMs available");
    }
    bool has_gpu_candidate = false;
    bool has_nongpu_candidate = false;

    for(const auto vm_id : vms) {
        if(!CanHostTask(vm_id, task_id)) continue;
        MachineInfo_t machine_info = Machine_GetInfo(VM_GetInfo(vm_id).machine_id);
        if(machine_info.gpus) has_gpu_candidate = true;
        else has_nongpu_candidate = true;
    }

    for(unsigned offset = 0; offset < vm_count; offset++) {
        unsigned idx = (round_robin_cursor + offset) % vm_count;
        VMId_t vm_id = vms[idx];
        if(!CanHostTask(vm_id, task_id)) continue;
        MachineInfo_t machine_info = Machine_GetInfo(VM_GetInfo(vm_id).machine_id);
        if(task_gpu_capable && has_gpu_candidate && !machine_info.gpus) continue;
        if(!task_gpu_capable && has_nongpu_candidate && machine_info.gpus) continue;
        round_robin_cursor = (idx + 1) % vm_count;
        return vm_id;
    }

    return INVALID_VM;
}

VMId_t Scheduler::SelectVMPMapper(TaskId_t task_id) const {
    const SLAType_t required_sla = RequiredSLA(task_id);
    const bool task_gpu_capable = IsTaskGPUCapable(task_id);
    const bool latency_sensitive = (required_sla == SLA0 || required_sla == SLA1);

    VMId_t best_vm = vms[0];
    bool found = false;
    double best_score = -1.0;
    bool has_gpu_candidate = false;
    bool has_nongpu_candidate = false;

    for(const auto vm_id : vms) {
        if(!CanHostTask(vm_id, task_id)) continue;
        MachineInfo_t machine_info = Machine_GetInfo(VM_GetInfo(vm_id).machine_id);
        if(machine_info.gpus) has_gpu_candidate = true;
        else has_nongpu_candidate = true;
    }

    for(const auto vm_id : vms) {
        if(!CanHostTask(vm_id, task_id)) continue;

        VMInfo_t vm_info = VM_GetInfo(vm_id);
        MachineInfo_t machine_info = Machine_GetInfo(vm_info.machine_id);
        if(task_gpu_capable && has_gpu_candidate && !machine_info.gpus) continue;
        if(!task_gpu_capable && has_nongpu_candidate && machine_info.gpus) continue;
        unsigned p_idx = static_cast<unsigned>(machine_info.p_state);

        double perf = 1.0;
        if(p_idx < machine_info.performance.size()) perf = static_cast<double>(machine_info.performance[p_idx]);

        double power = 1.0;
        if(!machine_info.s_states.empty()) power = static_cast<double>(machine_info.s_states[0]);
        if(p_idx < machine_info.p_states.size()) power += machine_info.num_cpus * static_cast<double>(machine_info.p_states[p_idx]);

        double perf_per_watt = perf / power;
        double load_bonus = 1.0 / (1.0 + static_cast<double>(machine_info.active_tasks));
        double score = latency_sensitive ? (0.7 * perf_per_watt + 0.3 * load_bonus)
                                         : (0.9 * perf_per_watt + 0.1 * load_bonus);

        if(!found || score > best_score) {
            found = true;
            best_score = score;
            best_vm = vm_id;
        }
    }

    if(found) return best_vm;
    return INVALID_VM;
}

VMId_t Scheduler::SelectVMEEco(TaskId_t task_id) const {
    const SLAType_t required_sla = RequiredSLA(task_id);
    const bool task_gpu_capable = IsTaskGPUCapable(task_id);
    const bool latency_sensitive = (required_sla == SLA0 || required_sla == SLA1 || required_sla == SLA2);

    VMId_t best_vm = vms[0];
    bool found = false;
    double best_score = -1.0;
    bool has_gpu_candidate = false;
    bool has_nongpu_candidate = false;

    for(const auto vm_id : vms) {
        if(!CanHostTask(vm_id, task_id)) continue;
        MachineInfo_t machine_info = Machine_GetInfo(VM_GetInfo(vm_id).machine_id);
        if(machine_info.gpus) has_gpu_candidate = true;
        else has_nongpu_candidate = true;
    }

    for(const auto vm_id : vms) {
        if(!CanHostTask(vm_id, task_id)) continue;

        VMInfo_t vm_info = VM_GetInfo(vm_id);
        MachineInfo_t machine_info = Machine_GetInfo(vm_info.machine_id);
        if(task_gpu_capable && has_gpu_candidate && !machine_info.gpus) continue;
        if(!task_gpu_capable && has_nongpu_candidate && machine_info.gpus) continue;
        unsigned p_idx = static_cast<unsigned>(machine_info.p_state);

        double machine_power = machine_info.s_states.empty() ? 1.0 : static_cast<double>(machine_info.s_states[0]);
        double cpu_power = (p_idx < machine_info.p_states.size()) ?
                           machine_info.num_cpus * static_cast<double>(machine_info.p_states[p_idx]) : 1.0;
        double power = machine_power + cpu_power;

        double score = 0.0;
        if(latency_sensitive) {
            score = (1.0 / power) + (1.0 / (1.0 + static_cast<double>(machine_info.active_tasks)));
        } else {
            // E-ECO: consolidate while preferring lower instantaneous power machines.
            score = (1.0 / power) + 0.05 * static_cast<double>(machine_info.active_tasks);
        }
        if(!found || score > best_score) {
            found = true;
            best_score = score;
            best_vm = vm_id;
        }
    }

    if(found) return best_vm;
    return INVALID_VM;
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}
