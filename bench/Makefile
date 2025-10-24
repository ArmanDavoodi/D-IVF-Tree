# Makefile for compiling the benchmark

# todo adding necessary flags for gdb at least when mode is debug
# todo changing the core location after making gdb work

# ================================
# Configurable build options
# ================================

ENABLE_GDB       ?= 1# Enables -g3
ENABLE_ASAN      ?= 1# Enables AddressSanitizer
ENABLE_DEBUGINFO ?= 1# Enables -rdynamic, -fno-omit-frame-pointer and debug-friendly flags
ENABLE_OPT       ?= 0# Enables optimization (-O3 vs -O0)

# ================================
# Common
# ================================

ROOT := $(shell dirname $(CURDIR))
DIVFTREE_PATH := $(ROOT)/include
INCLUDE_PATHS := -I$(DIVFTREE_PATH) -I$(ROOT)

BUILD_DIR := $(ROOT)/bench/build
OBJ_DIR := $(BUILD_DIR)/obj
BIN_DIR := $(BUILD_DIR)/bin

SRC_DIR := $(ROOT)/bench
$(info Source directory: $(SRC_DIR))

CXX := g++
WARNING_FLAGS := -Wall -Wextra -pedantic -Wno-pedantic -Wno-write-strings -Wno-pointer-arith
CXXFLAGS := $(WARNING_FLAGS) -std=c++20 $(INCLUDE_PATHS) -march=native -mcx16 -MMD -MP
LDFLAGS := -latomic -ldl

# ================================
# Conditional configuration
# ================================
# Optimization level
ifeq ($(ENABLE_OPT),1)
    CXXFLAGS += -O3
else
    CXXFLAGS += -O0
endif

# Debug info
ifeq ($(ENABLE_DEBUGINFO),1)
    CXXFLAGS += -fno-omit-frame-pointer
    LDFLAGS  += -fno-omit-frame-pointer -rdynamic
endif

ifeq ($(ENABLE_GDB),1)
    CXXFLAGS += -g3
endif

# AddressSanitizer
ifeq ($(ENABLE_ASAN),1)
    CXXFLAGS += -fsanitize=address
    LDFLAGS  += -fsanitize=address
endif

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