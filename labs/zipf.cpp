#include "zipf.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>

std::vector<ZipfRow> build_zipf_rows(const InvertedIndex &index) {
    std::vector<std::pair<std::string, TokenInfo>> items(index.tokens().begin(), index.tokens().end());
    std::sort(items.begin(), items.end(), [](const auto &a, const auto &b) {
        if (a.second.cf != b.second.cf) return a.second.cf > b.second.cf;
        return a.first < b.first;
    });

    std::vector<ZipfRow> rows;
    rows.reserve(items.size());
    if (items.empty()) return rows;

    double top_freq = static_cast<double>(items.front().second.cf);
    uint64_t rank = 0;
    for (const auto &kv : items) {
        ++rank;
        double r = static_cast<double>(rank);
        double freq = static_cast<double>(kv.second.cf);
        double expected = top_freq / r;
        rows.push_back({rank, kv.first, kv.second.cf,
                        std::log10(r), std::log10(freq),
                        expected, std::log10(expected)});
    }
    return rows;
}

void save_zipf_tsv(const std::string &path, const std::vector<ZipfRow> &rows) {
    std::ofstream out(path);
    out << "rank\ttoken\tfreq\tlog_rank\tlog_freq\tzipf_expected\tlog_zipf_expected\n";
    for (const auto &row : rows) {
        out << row.rank << '\t' << row.token << '\t' << row.freq << '\t'
            << row.log_rank << '\t' << row.log_freq << '\t'
            << row.zipf_expected << '\t' << row.log_zipf_expected << '\n';
    }
}
