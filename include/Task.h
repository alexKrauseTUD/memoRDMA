#ifndef MEMORDMA_TASK_H
#define MEMORDMA_TASK_H

#include <string>
#include <functional>

typedef std::function<void()> TaskFunction;

class Task {
    public:
        Task( std::string _ident, std::string _name, TaskFunction _run );
        ~Task();
        
    std::string ident;
    std::string name;
    TaskFunction run;
};

#endif // MEMORDMA_TASK_H