#ifndef INPUT_H_
#define INPUT_H_

#include <string>
#include <string.h>
#include <set>
#include <regex>
#include <stdio.h>
#include <vector>

struct UT_Input {
    std::set<std::string> black_list;
    std::set<std::string> white_list;
    std::regex* fmt = nullptr;
    std::regex* bfmt = nullptr;

    std::set<std::string> _tests;

    size_t num_tries = 1;

    static constexpr size_t MAX_NUM_TRIES = 1000;

    UT_Input(const std::set<std::string>& tests) : _tests(tests) {}

    ~UT_Input() {
        if (fmt != nullptr) {
            delete fmt;
        }

        if (bfmt != nullptr) {
            delete bfmt;
        }
    }

    inline void Get_Tests_To_Run(std::vector<std::string>& tests_to_run) {
        tests_to_run.clear();
        tests_to_run.reserve(_tests.size());

        for (std::string t : _tests) {
            if (fmt != nullptr && std::regex_match(t.c_str(), *fmt)) {
                white_list.insert(t);
            }
            if (bfmt != nullptr && std::regex_match(t.c_str(), *bfmt)) {
                black_list.insert(t);
            }
        }

        fprintf(stderr, "\n____________________________\n");
        fprintf(stderr, "# tries: %lu\nTests to run:\n", num_tries);
        for (std::string t : white_list) {
            if (!_tests.contains(t) || black_list.contains(t)) {
                continue;
            }
            tests_to_run.push_back(t);
            fprintf(stderr, "\t* %s\n", t.c_str());
        }
        fprintf(stderr, "\n");
        fprintf(stderr, "End of parse input.\n\n");
    }
};

enum UT_Parse_State {
    LIST,
    BLACK_LIST,
    WHITE_LIST,
    BFMT,
    FMT,
    NUM_TRIES,
    NAMES,
    HELP,
    INV,
    NUM_STATES
};

void Print_Usage() {
    fprintf(stderr, "Usage: [[-r \"<regex_fmt_str>\" ] [-u \"<regex_fmt_str>\"]"
            " [ -w <test_name>... ] [ -b <test_name>... ] [-n <num tries>]] | -l | -h | <test_name>... \n");
}

void Print_Help() {
    fprintf(stderr, " * If no input is given, runs all of the tests except the tests in the default black list.\n");
    fprintf(stderr, " * If no options are used, will only run the given tests.\n");
    fprintf(stderr, " * Black list and -u flags take priority over white list and -r\n");
    fprintf(stderr, " * Input regex syntax should follow the syntax of egrep regex.\n");
    fprintf(stderr, " * If -l or -h options are used, no tests will be run.\n");

    fprintf(stderr, "\t-r\t\tOnly runs the tests with names matching the input regex string.\n");
    fprintf(stderr, "\t-u\t\tWill not run the tests with names matching the input regex string.\n");
    fprintf(stderr, "\t-w\t\tWill run all of the tests in front of the option.\n");
    fprintf(stderr, "\t-b\t\tWill not run any of the tests in front of the option.\n");
    fprintf(stderr, "\t-n\t\tNumber of times the test will be run. Default value is 1. Should be in range [1, %lu].\n", 
            UT_Input::MAX_NUM_TRIES);
    fprintf(stderr, "\t-l\t\tWill list all of the tests.\n");
    fprintf(stderr, "\t-h\t\tPrints this message.\n");
}

inline UT_Parse_State Parse_Flag(const char& f) {
    switch (f) {
        case 'r':
            return FMT;
        case 'u':
            return BFMT;
        case 'w':
            return WHITE_LIST;
        case 'b':
            return BLACK_LIST;
        case 'n':
            return NUM_TRIES;
        case 'l':
            return NAMES;
        case 'h':
            return HELP;
        default:
            return INV;
    }
}

size_t Parse_Args(int argc, char *argv[], const std::set<std::string>& all_tests, 
                std::vector<std::string>& tests_to_run, const std::set<std::string>& default_black_list) {
    fprintf(stderr, "Start parse input...\n");

    UT_Input input(all_tests);
    UT_Parse_State state = INV;
    bool seen_option[NUM_STATES] = {false};
    bool non_list_option = false;
    bool use_default = true;

    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg.size() == 2 || arg[0] == '-') {
            state = Parse_Flag(arg[1]);
        }
        else if (!non_list_option) {
            state = LIST;
        }
        else {
            fprintf(stderr, "Fatal: Invalid input format.\n");
            Print_Usage();
            return 0;
        }

        switch (state) {
        case FMT:
            if (seen_option[FMT] || seen_option[LIST] || i+1 >= argc || 
                strlen(argv[i+1]) < 3 || (argv[i+1][0] != '\"' && argv[i+1][strlen(argv[i+1])-1] != '\"')) {

                fprintf(stderr, "Fatal: Invalid input format.\n");
                Print_Usage();
                return 0;
            }

            ++i;
            non_list_option = true;
            use_default = false;
            arg = std::string(argv[i] + 1, argv[i] + strlen(argv[i]) - 1);
            input.fmt = new std::regex(arg.c_str(), std::regex_constants::egrep);
            break;

        case BFMT:
            if (seen_option[BFMT] || seen_option[LIST] || i+1 >= argc || 
                strlen(argv[i+1]) < 3 || (argv[i+1][0] != '\"' && argv[i+1][strlen(argv[i+1])-1] != '\"')) {

                fprintf(stderr, "Fatal: Invalid input format.\n");
                Print_Usage();
                return 0;
            }

            ++i;
            non_list_option = true;
            use_default = false;
            arg = std::string(argv[i] + 1, argv[i] + strlen(argv[i]) - 1);
            input.bfmt = new std::regex(arg.c_str(), std::regex_constants::egrep);
            break;
        
        case WHITE_LIST:
            if (seen_option[WHITE_LIST] || seen_option[LIST] || i+1 >= argc) {
                fprintf(stderr, "Fatal: Invalid input format.\n");
                Print_Usage();
                return 0;
            }

            non_list_option = true;
            use_default = false;
            ++i;
            for (; i < argc; ++i) {
                arg = std::string(argv[i]);
                if (arg.size() == 2 || arg[0] == '-') {
                    --i;
                    break;
                }

                input.white_list.insert(arg);
            }
            
            break;

        case BLACK_LIST:
            if (seen_option[BLACK_LIST] || seen_option[LIST] || i+1 >= argc) {
                fprintf(stderr, "Fatal: Invalid input format.\n");
                Print_Usage();
                return 0;
            }

            non_list_option = true;
            use_default = false;
            ++i;
            for (; i < argc; ++i) {
                arg = std::string(argv[i]);
                if (arg.size() == 2 || arg[0] == '-') {
                    --i;
                    break;
                }

                input.black_list.insert(arg);
            }
            
            break;
        
        case NUM_TRIES:
            if (seen_option[NUM_TRIES] || i+1 >= argc) {
                fprintf(stderr, "Fatal: Invalid input format.\n");
                Print_Usage();
                return 0;
            }

            ++i;
            arg = std::string(argv[i]);
            if (arg.empty() || std::all_of(arg.begin(), arg.end(), ::isdigit)) {
                fprintf(stderr, "Fatal: Invalid input format.\n");
                Print_Usage();
                return 0;
            }
            
            input.num_tries = std::stoul(arg);
            if (input.num_tries == 0 || input.num_tries > UT_Input::MAX_NUM_TRIES) {
                fprintf(stderr, "Fatal: Number of tries hould be in range [1, %lu].\n", UT_Input::MAX_NUM_TRIES);
                Print_Usage();
                return 0;
            }

            break;

        case NAMES:
            if (seen_option[NAMES]) {
                fprintf(stderr, "Fatal: Invalid input format.\n");
                Print_Usage();
                return 0;
            }

            fprintf(stderr, "\n##########################\n");
            fprintf(stderr, "Tests:\n");
            for (const std::string& t : input._tests) {
                fprintf(stderr, " * %s\n", t.c_str());
            }
            fprintf(stderr, "##########################\n\n");

            break;
        
        case HELP:
            if (seen_option[HELP]) {
                fprintf(stderr, "Fatal: Invalid input format.\n");
                Print_Usage();
                return 0;
            }

            fprintf(stderr, "\n");
            Print_Help();
            fprintf(stderr, "\n");

            break;
        
        case LIST:
            if (seen_option[LIST] || non_list_option || i+1 >= argc) {
                fprintf(stderr, "Fatal: Invalid input format.\n");
                Print_Usage();
                return 0;
            }

            ++i;
            use_default = false;
            for (; i < argc; ++i) {
                arg = std::string(argv[i]);
                if (arg.size() == 2 || arg[0] == '-') {
                    --i;
                    break;
                }

                input.white_list.insert(arg);
            }
            
            break;
        
        case INV:
            fprintf(stderr, "Fatal: Invalid flag %s.\n", arg);
            Print_Usage();
            return 0;
        
        default:
            fprintf(stderr, "Fatal: Invalid input format.\n");
            Print_Usage();
            return 0;
        }

        seen_option[state] = true;
    }

    if (use_default) {
        input.black_list = default_black_list;
    }

    input.Get_Tests_To_Run(tests_to_run);
    return input.num_tries;
} 

#endif
