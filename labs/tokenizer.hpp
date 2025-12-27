#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

struct Document {
    uint64_t id{0};
    std::string source;
    std::string url;
    std::string title;
    std::string text;
};

struct TokenizationStats {
    uint64_t docs{0};
    uint64_t tokens{0};
    uint64_t token_chars{0};
    uint64_t bytes_in{0};
};

bool is_token_char(unsigned char c);
std::string strip_tags(const std::string &html);
void tokenize_document(const Document &doc, std::unordered_map<std::string, uint32_t> &freqs, TokenizationStats &stats);
