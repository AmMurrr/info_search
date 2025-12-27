#include "stemmer.hpp"

#include <cstring>

std::string stem_word(const std::string &word) {
    static const char *endings[] = {
        "овать", "ывать", "ировать", "ующий", "ation",
        "иями", "ями", "иями", "ения", "ении",
        "ость", "ости", "ост", "остью",
        "ский", "ская", "ское", "ские",
        "ого", "ему", "ыми", "его", "ому",
        "ать", "ять", "ить", "еть",
        "ом", "ам", "ах", "ях",
        "ой", "ей", "ый", "ий",
        "ов", "ев", "ам", "ям",
        "ы", "и", "е", "я", "ю", "ь",
        nullptr
    };

    if (word.size() < 4) return word;

    for (int i = 0; endings[i] != nullptr; ++i) {
        const char *suf = endings[i];
        size_t len = std::strlen(suf);
        if (word.size() > len + 1 && word.compare(word.size() - len, len, suf) == 0) {
            return word.substr(0, word.size() - len);
        }
    }
    return word;
}
