#include "Task.h"

Task::Task(std::string _ident, std::string name, TaskFunction _run) : ident(_ident),
                                                                      name(name),
                                                                      run(_run) {
}

Task::~Task() {
}