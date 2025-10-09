#ifndef CONFIG_READER_H_
#define CONFIG_READER_H_

#include "benchmark.h"

#include <fstream>
#include <string>
#include <algorithm>
#include <cctype>
#include <unordered_map>
#include <sstream>
#include <charconv>
#include <typeinfo>

inline constexpr char config_path[] = "run.conf";

inline std::unordered_map<std::string, std::string> var_configs;
inline std::unordered_map<std::string, std::vector<std::string>> list_configs;

void ReadConfigs() {
    std::ifstream file(config_path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open the config file");
    }

    std::string line;
    while (std::getline(file, line)) {
        /* remove all whitespace */
        line.erase(std::remove_if(line.begin(), line.end(), ::isspace), line.end());
        /* convert all characters to lower case */
        std::transform(line.begin(), line.end(), line.begin(), [](unsigned char c){ return std::tolower(c); });

        // Split into name and value
        auto pos = line.find(':');
        if (pos == std::string::npos)
            continue; // invalid line, skip

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        // Parse value type
        if (!value.empty() && value.front() == '[' && value.back() == ']') {
            // Parse list of numbers
            value = value.substr(1, value.size() - 2); // remove brackets
            std::vector<std::string> list;
            std::stringstream ss(value);
            std::string segment;
            while (std::getline(ss, segment, ',')) {
                if (!segment.empty()) {
                    list.push_back(segment);
                }
            }
            list_configs[key] = std::move(list);
        } else if (!value.empty()) {
            var_configs[key] = std::move(value);
        }
    }

    file.close();
}

template <typename T>
bool parseUnsignedInt(const std::string& s, T& value) {
    static_assert(std::is_unsigned_v<T>, "T must be an unsigned integer type");

    // Parse into the widest type (uint64_t) to detect overflow safely
    uint64_t temp = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), temp);

    if (ec != std::errc() || ptr != s.data() + s.size()) {
        // Parsing failed or extra characters were found
        return false;
    }

    if (temp > std::numeric_limits<T>::max()) {
        // Out of range for the target type
        return false;
    }

    value = static_cast<T>(temp);
    return true;
}

void ParseConfigs() {
    if (var_configs.empty() || list_configs.empty()) {
        throw std::runtime_error("Configs not available!");
    }

    index_attr.dimension = DIMENSION;
    if (index_attr.dimension == 0) {
        throw std::runtime_error("Invalid dimension");
    }

    index_attr.distanceAlg = DISTANCE_ALG;
    if (!divftree::IsValid(index_attr.distanceAlg)) {
        throw std::runtime_error("Invalid distance!");
    }

    auto vit = var_configs.find("log-path");
    if (vit != var_configs.end()) {
        divftree::debug::output_log = fopen(vit->second.c_str(), "w");
        if (divftree::debug::output_log == nullptr) {
            throw std::runtime_error(
                    divftree::String("Could not open the output log file! errno %d, errno msg: %s",
                                     errno, strerror(errno)).ToCStr());
        }
    }

    vit = var_configs.find("clustering");
    if (vit != var_configs.end()) {
        index_attr.clusteringAlg = divftree::CLUSTERING_NAME_TO_ENUM(vit->second.c_str());
        if (index_attr.clusteringAlg == divftree::ClusteringType::Invalid) {
            throw std::runtime_error("Invalid clustering type!");
        }
    } else {
        throw std::runtime_error("No clustering algorithm provided!");
    }

    auto lit = list_configs.find("leaf-size");
    if (lit != list_configs.end()) {
        if (lit->second.size() != 2) {
            throw std::runtime_error("Invalid leaf cluster size!");
        }

        if (!parseUnsignedInt(lit->second[0], index_attr.leaf_min_size)) {
            throw std::runtime_error("Invalid leaf min size!");
        }

        if (!parseUnsignedInt(lit->second[1], index_attr.leaf_max_size)) {
            throw std::runtime_error("Invalid leaf max size!");
        }

        if ((index_attr.leaf_max_size <= index_attr.leaf_min_size) ||
            (index_attr.leaf_max_size / 2 <= index_attr.leaf_min_size)) {
            throw std::runtime_error("Invalid leaf size range!");
        }
    } else {
        throw std::runtime_error("Leaf Cluster sizes not provided!");
    }

    lit = list_configs.find("internal-size");
    if (lit != list_configs.end()) {
        if (lit->second.size() != 2) {
            throw std::runtime_error("Invalid internal cluster size!");
        }

        if (!parseUnsignedInt(lit->second[0], index_attr.internal_min_size)) {
            throw std::runtime_error("Invalid internal min size!");
        }

        if (!parseUnsignedInt(lit->second[1], index_attr.internal_max_size)) {
            throw std::runtime_error("Invalid internal max size!");
        }

        if ((index_attr.internal_max_size <= index_attr.internal_min_size) ||
            (index_attr.internal_max_size / 2 <= index_attr.internal_min_size)) {
            throw std::runtime_error("Invalid internal size range!");
        }
    } else {
        throw std::runtime_error("Internal Cluster sizes not provided!");
    }

    vit = var_configs.find("leaf-split");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.split_leaf)) {
            throw std::runtime_error("Invalid leaf split factor!");
        }
    } else {
        throw std::runtime_error("Leaf split not provided!");
    }

    vit = var_configs.find("internal-split");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.split_internal)) {
            throw std::runtime_error("Invalid internal split factor!");
        }
    } else {
        throw std::runtime_error("Internal split not provided!");
    }

    vit = var_configs.find("leaf-block-bytes");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.leaf_blck_size) ||
            index_attr.leaf_blck_size < index_attr.dimension * sizeof(divftree::VTYPE)) {
            throw std::runtime_error("Invalid leaf block size!");
        }
    } else {
        throw std::runtime_error("Leaf block size not provided!");
    }

    vit = var_configs.find("internal-block-bytes");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.internal_blck_size) ||
            index_attr.internal_blck_size < index_attr.dimension * sizeof(divftree::VTYPE)) {
            throw std::runtime_error("Invalid internal block size!");
        }
    } else {
        throw std::runtime_error("Internal block size not provided!");
    }

    vit = var_configs.find("default-leaf-search-span");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, default_leaf_search_span) ||
            default_leaf_search_span < 1) {
            throw std::runtime_error("Invalid leaf search span!");
        }
    } else {
        throw std::runtime_error("Leaf search span not provided!");
    }

    vit = var_configs.find("default-internal-search-span");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, default_internal_search_span) ||
            default_internal_search_span < 1) {
            throw std::runtime_error("Invalid internal search span!");
        }
    } else {
        throw std::runtime_error("Internal search span not provided!");
    }

    vit = var_configs.find("random-rate-base");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.random_base_perc)) {
            throw std::runtime_error("Invalid random rate base!");
        }
    } else {
        throw std::runtime_error("Random rate base not provided!");
    }

    vit = var_configs.find("migration-check-trigger-rate");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.migration_check_triger_rate) ||
            (index_attr.migration_check_triger_rate > index_attr.random_base_perc)) {
            throw std::runtime_error("Invalid migration check trigger rate!");
        }
    } else {
        throw std::runtime_error("Migration rate trigger rate not provided!");
    }

    vit = var_configs.find("migration-check-trigger-single-rate");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.migration_check_triger_single_rate) ||
            (index_attr.migration_check_triger_single_rate > index_attr.random_base_perc)) {
            throw std::runtime_error("Invalid migration check trigger single rate!");
        }
    } else {
        throw std::runtime_error("Migration rate trigger single rate not provided!");
    }

    vit = var_configs.find("num-client-threads");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, num_threads) || (num_threads < 1)) {
            throw std::runtime_error("Invalid number of client threads!");
        }
    } else {
        throw std::runtime_error("Number of client threads not provided!");
    }

    vit = var_configs.find("num-searcher-threads");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.num_searchers)) {
            throw std::runtime_error("Invalid number of searcher threads!");
        }
    } else {
        throw std::runtime_error("Number of searcher threads not provided!");
    }

    vit = var_configs.find("num-bg-migrator-threads");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.num_migrators)) {
            throw std::runtime_error("Invalid number of migrator threads!");
        }
    } else {
        throw std::runtime_error("Number of migrator threads not provided!");
    }

    vit = var_configs.find("num-bg-merger-threads");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.num_mergers)) {
            throw std::runtime_error("Invalid number of merger threads!");
        }
    } else {
        throw std::runtime_error("Number of merger threads not provided!");
    }

    vit = var_configs.find("num-bg-compactor-threads");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, index_attr.num_compactors)) {
            throw std::runtime_error("Invalid number of compactor threads!");
        }
    } else {
        throw std::runtime_error("Number of compactor threads not provided!");
    }

    vit = var_configs.find("write-ratio");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, write_ratio) || (write_ratio > 100)) {
            throw std::runtime_error("Invalid write ratio!");
        }
    } else {
        throw std::runtime_error("Write ratio not provided!");
    }

    vit = var_configs.find("build-time");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, build_time)) {
            throw std::runtime_error("Invalid build time!");
        }
    } else {
        throw std::runtime_error("Build time not provided!");
    }

    vit = var_configs.find("warmup-time");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, warmup_time)) {
            throw std::runtime_error("Invalid warmup time!");
        }
    } else {
        throw std::runtime_error("Warmup time not provided!");
    }

    vit = var_configs.find("run-time");
    if (vit != var_configs.end()) {
        if (!parseUnsignedInt(vit->second, run_time)) {
            throw std::runtime_error("Invalid run time!");
        }
    } else {
        throw std::runtime_error("Run time not provided!");
    }
}

#endif