#include "loader.hpp"

#include <cstdint>
#include <fstream>
#include <iostream>

namespace {
char hex_to_char(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return 0;
}

std::string json_unescape(const std::string &line, size_t start) {
    std::string out;
    bool esc = false;
    for (size_t i = start; i < line.size(); ++i) {
        char c = line[i];
        if (esc) {
            switch (c) {
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                case '/': out.push_back('/'); break;
                case 'b': out.push_back('\b'); break;
                case 'f': out.push_back('\f'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                case 'u': {
                    if (i + 4 < line.size()) {
                        char h1 = hex_to_char(line[i + 1]);
                        char h2 = hex_to_char(line[i + 2]);
                        char h3 = hex_to_char(line[i + 3]);
                        char h4 = hex_to_char(line[i + 4]);
                        uint16_t code = (h1 << 12) | (h2 << 8) | (h3 << 4) | h4;
                        if (code < 0x80) {
                            out.push_back(static_cast<char>(code));
                        } else if (code < 0x800) {
                            out.push_back(static_cast<char>(0xC0 | (code >> 6)));
                            out.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                        } else {
                            out.push_back(static_cast<char>(0xE0 | (code >> 12)));
                            out.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
                            out.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                        }
                        i += 4;
                    }
                    break;
                }
                default:
                    out.push_back(c);
                    break;
            }
            esc = false;
        } else if (c == '\\') {
            esc = true;
        } else if (c == '"') {
            break;
        } else {
            out.push_back(c);
        }
    }
    return out;
}

bool extract_field(const std::string &line, const std::string &key, std::string &out) {
    const std::string needle = "\"" + key + "\"";
    size_t pos = line.find(needle);
    if (pos == std::string::npos) return false;
    pos = line.find('"', pos + needle.size());
    if (pos == std::string::npos) return false;
    out = json_unescape(line, pos + 1);
    return true;
}
}

std::vector<Document> load_documents_from_ndjson(const std::string &path) {
    std::ifstream in(path);
    if (!in) {
        std::cerr << "Cannot open input NDJSON: " << path << "\n";
        return {};
    }

    std::vector<Document> docs;
    std::string line;
    while (std::getline(in, line)) {
        Document doc;
        extract_field(line, "source", doc.source);
        extract_field(line, "url", doc.url);
        extract_field(line, "title", doc.title);
        std::string body;
        if (extract_field(line, "raw_html", body) || extract_field(line, "text", body) || extract_field(line, "content", body)) {
            doc.text = body;
        }
        if (doc.text.empty()) continue;
        doc.id = docs.size() + 1;
        if (doc.title.empty()) doc.title = doc.url;
        docs.push_back(std::move(doc));
    }
    return docs;
}

void process_ndjson_stream(const std::string &path, const std::function<void(Document &&)> &consumer,
                           size_t progress_every) {
    std::ifstream in(path);
    if (!in) {
        std::cerr << "Cannot open input NDJSON: " << path << "\n";
        return;
    }

    std::string line;
    size_t counter = 0;
    while (std::getline(in, line)) {
        Document doc;
        extract_field(line, "source", doc.source);
        extract_field(line, "url", doc.url);
        extract_field(line, "title", doc.title);
        std::string body;
        if (extract_field(line, "raw_html", body) || extract_field(line, "text", body) || extract_field(line, "content", body)) {
            doc.text = body;
        }
        if (doc.text.empty()) continue;
        counter += 1;
        doc.id = counter;
        if (doc.title.empty()) doc.title = doc.url;
        consumer(std::move(doc));
        if (progress_every && counter % progress_every == 0) {
            std::cout << "  [LOAD] processed " << counter << " documents" << std::endl;
        }
    }
}
