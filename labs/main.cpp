#include "index.hpp"
#include "loader.hpp"
#include "zipf.hpp"

#include <filesystem>
#include <iostream>

int main(int argc, char **argv) {
    std::string input_path = "data/all_docs.ndjson";
    std::string output_dir = "data";
    bool interactive = true;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--input=", 0) == 0) {
            input_path = arg.substr(8);
        } else if (arg.rfind("--output=", 0) == 0) {
            output_dir = arg.substr(9);
        } else if (arg == "--no-search") {
            interactive = false;
        }
    }

    if (!std::filesystem::exists(input_path)) {
        std::cerr << "Input file not found: " << input_path << "\n";
        std::cerr << "Run fetch_from_mongo.py first." << std::endl;
        return 1;
    }

    std::cout << "[INFO] Loading and indexing documents from " << input_path << "..." << std::endl;
    InvertedIndex index;
    process_ndjson_stream(input_path, [&](Document &&doc) {
        index.add_document(doc);
    }, 2000);

    if (index.doc_count() == 0) {
        std::cerr << "No documents loaded. Check input." << std::endl;
        return 1;
    }

    std::filesystem::create_directories(output_dir);
    std::string vocab_path = output_dir + "/vocabulary.tsv";
    std::string idx_path = output_dir + "/inverted_index.tsv";
    std::string docmap_path = output_dir + "/docs.tsv";
    std::string zipf_path = output_dir + "/zipf.tsv";

    index.save_vocabulary(vocab_path);
    index.save_inverted_index(idx_path);
    index.save_docmap(docmap_path);
    auto zipf_rows = build_zipf_rows(index);
    save_zipf_tsv(zipf_path, zipf_rows);

    const auto &st = index.stats();
    std::cout << "\n[STATS]\n";
    std::cout << "  Documents: " << st.docs << "\n";
    std::cout << "  Tokens:    " << st.tokens << "\n";
    std::cout << "  Unique:    " << index.vocab_size() << "\n";
    std::cout << "  Avg len:   " << (st.tokens ? (double)st.token_chars / st.tokens : 0.0) << "\n";
    std::cout << "  Bytes in:  " << st.bytes_in << "\n";

    std::cout << "\n[OUTPUT]\n";
    std::cout << "  " << vocab_path << "\n";
    std::cout << "  " << idx_path << "\n";
    std::cout << "  " << docmap_path << "\n";
    std::cout << "  " << zipf_path << "\n";

    if (!interactive) return 0;

    std::cout << "\nEnter boolean queries (use '&' for AND, '|' for OR). Empty line to exit." << std::endl;
    std::string query;
    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, query)) break;
        if (query.empty()) break;

        auto hits = index.search(query);
        if (hits.empty()) {
            std::cout << "No documents found." << std::endl;
            continue;
        }

        size_t limit = std::min<size_t>(hits.size(), 10);
        for (size_t i = 0; i < limit; ++i) {
            const auto &hit = hits[i];
            if (hit.doc_id == 0 || hit.doc_id > index.docs().size()) continue;
            const Document &doc = index.docs()[hit.doc_id - 1];
            std::cout << (i + 1) << ". doc " << hit.doc_id << " score=" << hit.score;
            if (!doc.title.empty()) std::cout << " | " << doc.title;
            if (!doc.url.empty()) std::cout << " | " << doc.url;
            std::cout << "\n";
        }
        if (hits.size() > limit) {
            std::cout << "... and " << (hits.size() - limit) << " more" << std::endl;
        }
    }

    return 0;
}
