# Makefile for compiling the benchmark

# todo adding necessary flags for gdb at least when mode is debug
# todo changing the core location after making gdb work

ROOT := $(CURDIR)/..
DIVFTREE_PATH = $(ROOT)/include
INCLUDE_PATHS = -I$(DIVFTREE_PATH) -I$(ROOT)

BUILD_DIR = $(ROOT)/bench/build
OBJ_DIR := $(BUILD_DIR)/obj
BIN_DIR := $(BUILD_DIR)/bin

SRC_DIR = $(ROOT)/bench
$(info Source directory: $(SRC_DIR))

CXX = g++
IGNORE_WARNING = -Wno-pedantic -Wno-write-strings -Wno-pointer-arith
CXXFLAGS = -std=c++20 $(INCLUDE_PATHS) -Wall -Wextra -pedantic -g -ldl -MMD -MP -rdynamic $(IGNORE_WARNING)

# Add custom defines if they are provided
ifneq ($(filter -D%,$(DEFINES)),)
    CXXFLAGS += $(filter -D%,$(DEFINES))
endif

TESTS = $(wildcard $(SRC_DIR)/*.cpp)
$(info Found test files: $(TESTS))

OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(OBJ_DIR)/%.o, $(TESTS))
DEPS = $(patsubst $(SRC_DIR)/%.cpp, $(OBJ_DIR)/%.d, $(TESTS))
EXES = $(patsubst $(SRC_DIR)/%.cpp, $(BIN_DIR)/%, $(TESTS))

all: $(EXES)

$(BIN_DIR)/%: $(OBJ_DIR)/%.o | $(BIN_DIR)
	$(CXX) $(LDFLAGS) -o $@ $<

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp | $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BIN_DIR): $(BUILD_DIR)
	mkdir -p $(BIN_DIR)

$(OBJ_DIR): $(BUILD_DIR)
	mkdir -p $(OBJ_DIR)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)


# Include dependency files (if they exist)
-include $(DEPS)

clean:
	rm -rf $(BUILD_DIR)

.PHONY: all clean