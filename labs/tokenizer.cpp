#include "tokenizer.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <unordered_set>

namespace {
const char *kStopwords[] = {
    "в", "и", "на", "с", "по", "для", "от", "к", "как", "это",
    "то", "он", "она", "оно", "они", "мы", "вы", "я", "ты", "мне",
    "тебе", "нам", "вам", "ему", "ей", "им", "их", "его", "её",
    "что", "кто", "где", "когда", "почему", "зачем", "который",
    "есть", "быть", "иметь", "делать", "идти", "приходить",
    "а", "но", "если", "то", "или", "либо", "же", "ли",
    "не", "ни", "нет", "никогда", "никто", "ничто",
    "более", "менее", "очень", "совсем", "почти", "всегда",
    "здесь", "там", "туда", "сюда", "отсюда",
    "новости", "ria", "ru", "https", "http", "com", "org", "net",
    "отправить", "класс", "onclick", "href", "src", "www",
    nullptr
};

std::unordered_set<std::string> BuildStopwordSet() {
    std::unordered_set<std::string> s;
    for (int i = 0; kStopwords[i] != nullptr; ++i) {
        s.insert(kStopwords[i]);
    }
    return s;
}

const std::unordered_set<std::string> &Stopwords() {
    static const std::unordered_set<std::string> stop = BuildStopwordSet();
    return stop;
}

char to_lower_ascii(char c) {
    if (c >= 'A' && c <= 'Z') return static_cast<char>(c + 32);
    return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
}
}

bool is_token_char(unsigned char c) {
    if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') return true;
    if (c >= 0x80) return true;
    return false;
}

std::string strip_tags(const std::string &html) {
    std::string out;
    out.reserve(html.size());
    bool in_tag = false;
    for (char c : html) {
        if (c == '<') {
            in_tag = true;
            continue;
        }
        if (c == '>' && in_tag) {
            in_tag = false;
            continue;
        }
        if (!in_tag) out.push_back(c);
    }
    return out;
}

void tokenize_document(const Document &doc, std::unordered_map<std::string, uint32_t> &freqs, TokenizationStats &stats) {
    std::string text = strip_tags(doc.text);
    stats.docs += 1;
    stats.bytes_in += text.size();

    std::string token;
    token.reserve(32);

    auto flush = [&]() {
        if (token.empty()) return;
        if (!token.empty()) {
            if (Stopwords().find(token) == Stopwords().end()) {
                ++freqs[token];
                stats.tokens += 1;
                stats.token_chars += token.size();
            }
        }
        token.clear();
    };

    for (unsigned char c : text) {
        if (is_token_char(c)) {
            token.push_back(to_lower_ascii(static_cast<char>(c)));
        } else {
            flush();
        }
    }
    flush();
}
