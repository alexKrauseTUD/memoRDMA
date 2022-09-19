#include <stdio.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <thread>

#include "ConnectionManager.h"
#include "TaskManager.h"
#include "common.h"
#include "util.h"
#include "FunctionalTests.hpp"
#include "Logger.h"

bool checkLinkUp() {
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen("ibstat", "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return (result.find( "State: Active" ) != std::string::npos);
}

using namespace memordma;

int main(int argc, char *argv[]) {
    // Init Config and stuff.
    Logger::LoadConfiguration();

    if ( !checkLinkUp() ) {
        Logger::getInstance() << LogLevel::FATAL << "Could not find 'Active' state in ibstat, please check! Maybe you need to run \"sudo opensm -B\" on any server." << std::endl;
        exit(-2);
    }

    bool abort = false;
    auto globalExit = [&]() -> void {
        {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(500ms);
        }
        ConnectionManager::getInstance().stop();
        abort = true;
    };

    TaskManager::getInstance().setGlobalAbortFunction(globalExit);

    // This is a bit dirty solution. It is needed for registering the callbacks on SU.
    FunctionalTests::getInstance();

    std::string content;
    std::string op;

    while (!abort) {
        op = "-1";
        TaskManager::getInstance().printAll();
        std::cout << "Type \"exit\" to terminate." << std::endl;
        // std::cin >> op;
        std::getline(std::cin, op, '\n');
        if (op == "-1") {
            globalExit();
            continue;
        }

        Logger::getInstance() << LogLevel::INFO << "Chosen:" << op << std::endl;
        std::transform(op.begin(), op.end(), op.begin(), [](unsigned char c) { return std::tolower(c); });

        if (op == "exit") {
            globalExit();
        } else {
            std::size_t id;
            bool converted = false;
            try {
                id = stol(op);
                converted = true;
            } catch (...) {
                Logger::getInstance() << LogLevel::ERROR << "No number given." << std::endl;
                continue;
            }
            if (converted) {
                TaskManager::getInstance().executeById(id);
            }
        }
    }

    return 0;
}
