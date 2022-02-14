#ifndef MEMORDMA_TASK_MANAGER_H
#define MEMORDMA_TASK_MANAGER_H

#include <map>
#include <string>

#include "Task.h"

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
};

#endif  // MEMORDMA_TASK_MANAGER_H