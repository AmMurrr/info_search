#pragma once

#include "stemmer.hpp"
#include "tokenizer.hpp"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct Posting {
    uint64_t doc_id;
    uint32_t tf;
};

struct TokenInfo {
    uint64_t cf{0};
    uint32_t df{0};
    std::vector<Posting> postings;
};

struct SearchHit {
    uint64_t doc_id;
    uint64_t score;
};

class InvertedIndex {
public:
    void add_document(const Document &doc);
    std::vector<SearchHit> search(const std::string &query) const;

    size_t vocab_size() const { return index_.size(); }
    size_t doc_count() const { return docs_.size(); }
    const TokenizationStats &stats() const { return stats_; }

    void save_vocabulary(const std::string &path) const;
    void save_inverted_index(const std::string &path) const;
    void save_docmap(const std::string &path) const;

    const std::unordered_map<std::string, TokenInfo> &tokens() const { return index_; }
    const std::vector<Document> &docs() const { return docs_; }

private:
    std::unordered_map<std::string, TokenInfo> index_;
    std::vector<Document> docs_;
    TokenizationStats stats_;
};
