#pragma once

#include "tokenizer.hpp"

#include <string>
#include <vector>
#include <functional>

std::vector<Document> load_documents_from_ndjson(const std::string &path);

// Stream NDJSON without keeping everything in memory; calls consumer for each parsed doc
void process_ndjson_stream(const std::string &path, const std::function<void(Document &&)> &consumer,
						   size_t progress_every = 5000);
