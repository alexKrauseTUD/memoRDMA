#ifndef MEMORDMA_TASK_MANAGER_H
#define MEMORDMA_TASK_MANAGER_H

#include <map>
#include <string>

#include "Task.h"
#include "common.h"

enum test_code {
    ss_tput,
    ds_tput,
    mt_ss_tput,
    mt_ds_tput
};


enum init_task_type {
    connection_handling = 1,
    buffer_handling = 1 << 1,
    dummy_tests = 1 << 2,
    performance_tests = 1 << 3
};

class TaskManager {
   private:
    TaskManager();

   public:
    static TaskManager &getInstance() {
        static TaskManager instance;
        return instance;
    }
    ~TaskManager();

    TaskManager(TaskManager const &) = delete;
    void operator=(TaskManager const &) = delete;

    void registerTask(Task *task);
    void unregisterTask( std::string ident );
    bool hasTask( std::string ident ) const;
    void printAll();

    void executeById(std::size_t id);
    void executeByIdent(std::string name);

    void setup( size_t init_flags );

    void setGlobalAbortFunction(std::function<void()> fn);

   private:
    std::map<std::size_t, Task *> tasks;
    std::size_t globalId;
    std::function<void()> globalAbort;

    void genericTestFunc(std::string shortName, std::string name, test_code tc, std::size_t connectionId, Strategies strat);
};

#endif  // MEMORDMA_TASK_MANAGER_H