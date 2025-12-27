#include "index.hpp"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>

namespace {
std::string trim(const std::string &s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
    return s.substr(start, end - start);
}

std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> parts;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        parts.push_back(item);
    }
    return parts;
}

std::string normalize_term(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        if (c >= 'A' && c <= 'Z') out.push_back(static_cast<char>(c + 32));
        else out.push_back(static_cast<char>(c));
    }
    return stem_word(out);
}
}

void InvertedIndex::add_document(const Document &doc) {
    std::unordered_map<std::string, uint32_t> freqs;
    tokenize_document(doc, freqs, stats_);
    Document meta = doc;
    meta.text.clear(); // free heavy text to keep memory low
    docs_.push_back(std::move(meta));

    for (const auto &kv : freqs) {
        std::string term = normalize_term(kv.first);
        auto &info = index_[term];
        info.cf += kv.second;
        info.df += 1;
        info.postings.push_back({doc.id, kv.second});
    }
}

std::vector<SearchHit> InvertedIndex::search(const std::string &query) const {
    std::vector<std::string> raw_groups = split(query, '|');
    std::unordered_set<uint64_t> final_set;
    bool first_group = true;

    for (const auto &raw_group : raw_groups) {
        std::vector<std::string> terms_raw = split(raw_group, '&');
        std::vector<std::string> terms;
        for (const auto &t : terms_raw) {
            std::string trimmed = trim(t);
            if (!trimmed.empty()) terms.push_back(normalize_term(trimmed));
        }
        if (terms.empty()) continue;

        std::unordered_set<uint64_t> group_set;
        bool first_term = true;
        for (const auto &term : terms) {
            auto it = index_.find(term);
            if (it == index_.end()) {
                group_set.clear();
                break;
            }
            std::unordered_set<uint64_t> term_set;
            term_set.reserve(it->second.postings.size());
            for (const auto &p : it->second.postings) term_set.insert(p.doc_id);

            if (first_term) {
                group_set = std::move(term_set);
                first_term = false;
            } else {
                std::unordered_set<uint64_t> intersect;
                intersect.reserve(std::min(group_set.size(), term_set.size()));
                for (auto id : group_set) {
                    if (term_set.find(id) != term_set.end()) intersect.insert(id);
                }
                group_set = std::move(intersect);
            }

            if (group_set.empty()) break;
        }

        if (group_set.empty()) continue;

        if (first_group) {
            final_set = std::move(group_set);
            first_group = false;
        } else {
            final_set.insert(group_set.begin(), group_set.end());
        }
    }

    if (final_set.empty()) return {};

    std::unordered_map<uint64_t, uint64_t> scores;
    scores.reserve(final_set.size());

    // Collect unique terms once
    std::unordered_set<std::string> unique_terms;
    for (const auto &raw_group : raw_groups) {
        for (const auto &t : split(raw_group, '&')) {
            std::string trimmed = trim(t);
            if (!trimmed.empty()) unique_terms.insert(normalize_term(trimmed));
        }
    }

    for (const auto &term : unique_terms) {
        auto it = index_.find(term);
        if (it == index_.end()) continue;
        for (const auto &p : it->second.postings) {
            if (final_set.find(p.doc_id) != final_set.end()) {
                scores[p.doc_id] += p.tf;
            }
        }
    }

    std::vector<SearchHit> hits;
    hits.reserve(final_set.size());
    for (auto id : final_set) {
        hits.push_back({id, scores[id]});
    }

    std::sort(hits.begin(), hits.end(), [](const SearchHit &a, const SearchHit &b) {
        if (a.score != b.score) return a.score > b.score;
        return a.doc_id < b.doc_id;
    });

    return hits;
}

void InvertedIndex::save_vocabulary(const std::string &path) const {
    std::vector<std::pair<std::string, TokenInfo>> items(index_.begin(), index_.end());
    std::sort(items.begin(), items.end(), [](const auto &a, const auto &b) {
        if (a.second.cf != b.second.cf) return a.second.cf > b.second.cf;
        return a.first < b.first;
    });

    std::ofstream out(path);
    out << "rank\ttoken\tcf\tdf\n";
    size_t rank = 0;
    for (const auto &kv : items) {
        ++rank;
        out << rank << '\t' << kv.first << '\t' << kv.second.cf << '\t' << kv.second.df << '\n';
    }
}

void InvertedIndex::save_inverted_index(const std::string &path) const {
    std::vector<std::string> tokens;
    tokens.reserve(index_.size());
    for (const auto &kv : index_) tokens.push_back(kv.first);
    std::sort(tokens.begin(), tokens.end());

    std::ofstream out(path);
    out << "token\tdoc_id\ttf\n";
    for (const auto &tok : tokens) {
        const auto &info = index_.at(tok);
        for (const auto &p : info.postings) {
            out << tok << '\t' << p.doc_id << '\t' << p.tf << '\n';
        }
    }
}

void InvertedIndex::save_docmap(const std::string &path) const {
    std::ofstream out(path);
    out << "doc_id\tsource\ttitle\turl\n";
    for (const auto &doc : docs_) {
        std::string safe_title = doc.title;
        std::replace(safe_title.begin(), safe_title.end(), '\t', ' ');
        out << doc.id << '\t' << doc.source << '\t' << safe_title << '\t' << doc.url << '\n';
    }
}
