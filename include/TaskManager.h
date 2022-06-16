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

class TaskManager {
   public:
    TaskManager();
    ~TaskManager();

    void registerTask(Task* task);
    void printAll();

    void executeById(std::size_t id);
    void executeByIdent(std::string name);

    void setup();

    void setGlobalAbortFunction(std::function<void()> fn);

   private:
    std::map<std::size_t, Task*> tasks;
    std::size_t globalId;
    std::function<void()> globalAbort;

    void genericTestFunc(std::string shortName, std::string name, test_code tc, std::size_t connectionId, Strategies strat);
};

#endif  // MEMORDMA_TASK_MANAGER_H