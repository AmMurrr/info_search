#pragma once

#include "index.hpp"

#include <string>
#include <vector>

struct ZipfRow {
    uint64_t rank;
    std::string token;
    uint64_t freq;
    double log_rank;
    double log_freq;
    double zipf_expected;
    double log_zipf_expected;
};

std::vector<ZipfRow> build_zipf_rows(const InvertedIndex &index);
void save_zipf_tsv(const std::string &path, const std::vector<ZipfRow> &rows);
