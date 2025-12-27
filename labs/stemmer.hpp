#pragma once

#include <string>

// Very small Russian stemmer based on suffix stripping
std::string stem_word(const std::string &word);
