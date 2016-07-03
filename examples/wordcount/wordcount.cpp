// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#define DEBUG_TRACE_OUTPUT
#define BOOST_DISABLE_ASSERTS 
#if !defined(_DEBUG) &&  !defined(BOOST_DISABLE_ASSERTS)
#   pragma message("Warning: BOOST_DISABLE_ASSERTS not defined")
#endif

#include <boost/config.hpp>

#if defined(BOOST_MSVC)
#   pragma warning(disable: 4100 4127 4244 4512 4267 4996)
#endif

#include "mapreduce.hpp"
#include <boost/algorithm/string.hpp>

#if defined(BOOST_MSVC)  &&  defined(_DEBUG)
#include <crtdbg.h>
#endif

#include "wordcount.h"
#include <iostream>

template<>
inline uintmax_t const mapreduce::length(std::pair<char const *, uintmax_t> const &string)
{
    return string.second;
}

template<>
inline char const * const mapreduce::data(std::pair<char const *, uintmax_t> const &string)
{
    return string.first;
}

template<>
inline
size_t const
mapreduce::hash_partitioner::operator()(
    std::pair<char const *, std::uintmax_t> const &key,
    size_t partitions) const
{
    return boost::hash_range(key.first, key.first+key.second) % partitions;
}

// use case insensitive string comparison for matching words
template<>
constexpr
bool std::less<std::pair<char const *, std::uintmax_t> >::operator()(
         std::pair<char const *, std::uintmax_t> const &first,
         std::pair<char const *, std::uintmax_t> const &second) const
{
    return (strnicmp(first.first, second.first, std::min(first.second, second.second)) < 0)
         || ((first.second < second.second)  &&  (strnicmp(first.first, second.first, std::min(first.second, second.second)) <= 0));
}

template<>
constexpr
bool std::less<std::string>::operator()(
         std::string const &first,
         std::string const &second) const
{
    return
        std::less<std::pair<char const *, std::uintmax_t>>()(
            std::pair<char const *, std::uintmax_t>(first.c_str(), first.length()),
            std::pair<char const *, std::uintmax_t>(second.c_str(), second.length()));
}

namespace {

template<typename T>
double const sum(T const &durations)
{
    double sum = 0.0;
    for (auto &chrono : durations)
        sum += chrono.count();
    return sum;
}

void write_stats(mapreduce::results const &result)
{
    if (result.map_times.size() == 0  || result.reduce_times.size() == 0)
        return;

    std::cout << std::endl << "\nMapReduce statistics:";
    std::cout << "\n  MapReduce job runtime                     : " << result.job_runtime.count() << "s of which...";
    std::cout << "\n    Map phase runtime                       : " << result.map_runtime.count() << "s";
    std::cout << "\n    Reduce phase runtime                    : " << result.reduce_runtime.count() << "s";
    std::cout << "\n\n  Map:";
    std::cout << "\n    Total Map keys                          : " << result.counters.map_keys_executed;
    std::cout << "\n    Map keys processed                      : " << result.counters.map_keys_completed;
    std::cout << "\n    Map key processing errors               : " << result.counters.map_key_errors;
    std::cout << "\n    Number of Map Tasks run (in parallel)   : " << result.counters.actual_map_tasks;
    std::cout << "\n    Fastest Map key processed in            : " << std::min_element(result.map_times.cbegin(), result.map_times.cend())->count() << "s";
    std::cout << "\n    Slowest Map key processed in            : " << std::max_element(result.map_times.cbegin(), result.map_times.cend())->count() << "s";
    std::cout << "\n    Average time to process Map keys        : " << sum(result.map_times) / double(result.map_times.size()) << "s";

    std::cout << "\n\n  Reduce:";
    std::cout << "\n    Total Reduce keys                       : " << result.counters.reduce_keys_executed;
    std::cout << "\n    Reduce keys processed                   : " << result.counters.reduce_keys_completed;
    std::cout << "\n    Reduce key processing errors            : " << result.counters.reduce_key_errors;
    std::cout << "\n    Number of Reduce Tasks run (in parallel): " << result.counters.actual_reduce_tasks;
    std::cout << "\n    Number of Result Files                  : " << result.counters.num_result_files;
    if (result.reduce_times.size() > 0)
    {
        std::cout << "\n    Fastest Reduce key processed in         : " << std::min_element(result.reduce_times.cbegin(), result.reduce_times.cend())->count() << "s";
        std::cout << "\n    Slowest Reduce key processed in         : " << std::max_element(result.reduce_times.cbegin(), result.reduce_times.cend())->count() << "s";
        std::cout << "\n    Average time to process Reduce keys     : " << sum(result.reduce_times) / double(result.map_times.size()) << "s";
    }
    std::cout << std::endl;
}

std::ostream &operator<<(std::ostream &o, std::pair<char const *, uintmax_t> const &str)
{
    std::copy(str.first, str.first+str.second, std::ostream_iterator<char>(o,""));
    return o;
}

template<typename Job>
void write_frequency_table(Job const &job)
{
    flush(std::cout);

    auto it  = job.begin_results();
    auto ite = job.end_results();
    if (it != ite)
    {
        std::cout << "\n\nMapReduce results:";

        using frequencies_t = std::list<typename Job::keyvalue_t>;
        frequencies_t frequencies;
        frequencies.push_back(*it);
        auto it_smallest = frequencies.rbegin();
        for (++it; it!=ite; ++it)
        {
            if (frequencies.size() < 1000)    // show top 10
            {
                frequencies.push_back(*it);
                if (it->second < it_smallest->second)
                    it_smallest = frequencies.rbegin();
            }
            else if (it->second > it_smallest->second)
            {
                *it_smallest = *it;

                it_smallest = std::min_element(
                    frequencies.rbegin(),
                    frequencies.rend(),
                    mapreduce::detail::less_2nd<typename Job::keyvalue_t>);
            }
        }

        frequencies.sort(mapreduce::detail::greater_2nd<typename Job::keyvalue_t>);
        for (auto &freq : frequencies)
            std::cout << "\n" << freq.first << "\t" << freq.second;
    }
}

template<typename Job>
void run_wordcount(mapreduce::specification const &spec)
{
    std::cout << "\n" << typeid(Job).name() << "\n";

    try
    {
        mapreduce::results result;
        typename Job::datasource_type datasource(spec);

        Job job(datasource, spec);
#ifdef _DEBUG
        std::cout << "\nRunning Sequential WordCount MapReduce...";
        job.run<mapreduce::schedule_policy::sequential<Job> >(result);
#else
        std::cout << "\nRunning Parallel WordCount MapReduce...";
        job.run<mapreduce::schedule_policy::cpu_parallel<Job> >(result);
#endif
        std::cout << "\nMapReduce Finished.";

        write_stats(result);
        write_frequency_table(job);
    }
    catch (std::exception &e)
    {
        std::cout << "\nError: " << e.what();
    }
}

}   // anonymous namespace

// specialized stream operator to read and write a key/value pair of the types of the reduce task
inline
std::basic_ostream<char, std::char_traits<char>> &
operator<<(
    std::basic_ostream<char, std::char_traits<char>>                   &out,
    std::pair<std::pair<char const *, std::uintmax_t>, unsigned> const &keyvalue)
{
    out << keyvalue.first.second << "\t";
    out.write(keyvalue.first.first, keyvalue.first.second);
    out << "\t" << keyvalue.second;
    return out;
}

inline
std::basic_ostream<char, std::char_traits<char>> &
operator<<(
    std::basic_ostream<char, std::char_traits<char>> &out,
    std::pair<std::string, unsigned>           const &keyvalue)
{
    out <<
        std::make_pair(
            std::make_pair(keyvalue.first.c_str(), keyvalue.first.length()),
            keyvalue.second);
    return out;
}

inline
std::basic_istream<char, std::char_traits<char>> &
operator>>(
    std::basic_istream<char, std::char_traits<char>> &in,
    std::pair<std::string, unsigned>                 &keyvalue)
{
    size_t length;
    in >> length;
    if (!in.eof()  &&  !in.fail())
    {
        char tab;
        in.read(&tab, 1); assert(tab == '\t');

        keyvalue.first.resize(length);
        in.read(&*keyvalue.first.begin(), length);
        in.read(&tab, 1); assert(tab == '\t');
        in >> keyvalue.second;
    }
    return in;
}


int main(int argc, char **argv)
{
#ifdef _CRTDBG_REPORT_FLAG
    _CrtSetDbgFlag(_CrtSetDbgFlag(_CRTDBG_REPORT_FLAG) | _CRTDBG_LEAK_CHECK_DF);
#endif

    std::cout << "MapReduce Word Frequency Application";
    if (argc < 2)
    {
        std::cerr << "Usage: wordcount directory [num_map_tasks]\n";
        return 1;
    }

    assert(!(
        std::less<std::pair<char const *, std::uintmax_t>>()(
            std::pair<char const *, std::uintmax_t>("Hello",5),
            std::pair<char const *, std::uintmax_t>("Hello",5))));
    assert((
        std::less<std::pair<char const *, std::uintmax_t>>()(
            std::pair<char const *, std::uintmax_t>("abc",3),
            std::pair<char const *, std::uintmax_t>("abcd",4))));
    assert(!(
        std::less<std::pair<char const *, std::uintmax_t>>()(
            std::pair<char const *, std::uintmax_t>("abcd",4),
            std::pair<char const *, std::uintmax_t>("abc",3))));
    assert((
        std::less<std::pair<char const *, std::uintmax_t>>()(
            std::pair<char const *, std::uintmax_t>("abc",3),
            std::pair<char const *, std::uintmax_t>("abd",3))));
    assert(!(
        std::less<std::pair<char const *, std::uintmax_t>>()(
            std::pair<char const *, std::uintmax_t>("abd",3),
            std::pair<char const *, std::uintmax_t>("abc",3))));

    mapreduce::specification spec;
    spec.input_directory = argv[1];

    if (argc > 2)
        spec.map_tasks = std::max(1, atoi(argv[2]));

    if (argc > 3)
        spec.reduce_tasks = std::max(1, atoi(argv[3]));
    else
        spec.reduce_tasks = std::max(1U, std::thread::hardware_concurrency());

    std::cout << "\n" << std::max(1U, std::thread::hardware_concurrency()) << " CPU cores";

    /*
      the tests before are in pairs; tests running without a
      functional combiner, and then with a combiner object
    */

    // test using a reduce key of a char pointer and length, to
    // a memory-mapped buffer of text. this will work only for
    // in-memory intermediates where the  memory-mapped buffer
    // lifetime exceeds the duration of the map reduce job
    run_wordcount<
        mapreduce::job<
            wordcount::map_task,
            wordcount::reduce_task<
                std::pair<char const *, std::uintmax_t>>> >(spec);

    run_wordcount<
        mapreduce::job<
            wordcount::map_task,
            wordcount::reduce_task<std::pair<char const *, std::uintmax_t>>,
            wordcount::combiner<
                wordcount::reduce_task<
                    std::pair<char const *, std::uintmax_t>>>>>(spec);

    // these are functionally the same as the jobs above, but use std::string
    // as the reduce key so the char buffer is owned by the intermediate store.
    // this is less efficient, but more robust if the memory-mapped buffer
    // may go out of scope
    run_wordcount<
        mapreduce::job<
            wordcount::map_task,
            wordcount::reduce_task<std::string>> >(spec);

    run_wordcount<
        mapreduce::job<
            wordcount::map_task,
            wordcount::reduce_task<std::string>,
            wordcount::combiner<
                wordcount::reduce_task<std::string>>>>(spec);

    // because the intermediates are stored on disk and read back during the reduce
    // phase, the reduce keys must own their own storage, so std::string is used
    run_wordcount<
        mapreduce::job<
            wordcount::map_task,
            wordcount::reduce_task<std::string>,
            mapreduce::null_combiner,
            mapreduce::datasource::directory_iterator<wordcount::map_task>,
            mapreduce::intermediates::local_disk<
                wordcount::map_task,
                wordcount::reduce_task<std::string>,
                wordcount::map_task::value_type,
                mapreduce::hash_partitioner,
                mapreduce::intermediates::reduce_file_output<wordcount::map_task, wordcount::reduce_task<std::string>>,
                mapreduce::detail::file_key_combiner<
                    wordcount::key_combiner<
                        std::pair<
                            wordcount::reduce_task<std::string>::key_type,
                            wordcount::reduce_task<std::string>::value_type>>
    >>>>(spec);

    run_wordcount<
        mapreduce::job<
            wordcount::map_task,
            wordcount::reduce_task<std::string>,
            wordcount::combiner<wordcount::reduce_task<std::string>>,
            mapreduce::datasource::directory_iterator<wordcount::map_task>,
            mapreduce::intermediates::local_disk<
                wordcount::map_task,
                wordcount::reduce_task<std::string>,
                wordcount::map_task::value_type,
                mapreduce::hash_partitioner,
                mapreduce::intermediates::reduce_file_output<wordcount::map_task, wordcount::reduce_task<std::string>>,
                mapreduce::detail::file_key_combiner<
                    wordcount::key_combiner<
                        std::pair<
                            wordcount::reduce_task<std::string>::key_type,
                            wordcount::reduce_task<std::string>::value_type>>
    >>>>(spec);

    return 0;
}

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
