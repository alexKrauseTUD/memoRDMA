#ifndef MEMORDMA_TASK_MANAGER_H
#define MEMORDMA_TASK_MANAGER_H

#include <map>
#include <string>

#include "Task.h"

enum test_code {
    ss_tput = 1,
    ds_tput = 2,
    mt_ss_tput = 3,
    mt_ds_tput = 4,
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
    void printAll();

    void executeById(std::size_t id);
    void executeByIdent(std::string name);

    void setup();

    void setGlobalAbortFunction(std::function<void()> fn);

   private:
    std::map<std::size_t, Task *> tasks;
    std::size_t globalId;
    std::function<void()> globalAbort;

    void genericTestFunc(std::string shortName, std::string name, test_code tc, std::size_t connectionId);
};

#endif  // MEMORDMA_TASK_MANAGER_H