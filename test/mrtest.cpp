// Boost.MapReduce library
//
//  Copyright (C) 2009 Craig Henderson.
//  cdm.henderson@googlemail.com
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// For more information, see http://www.boost.org/libs/mapreduce/
//

// configuration options
#define WORD_COUNT_MEMORY_MAP_FILE

#if defined(_DEBUG)
#   if 1
#       define RUN_SEQUENTIAL_MAP_REDUCE
#   endif
#else
#   define BOOST_DISABLE_ASSERTS
#endif
 
#if !defined(_DEBUG) &&  !defined(BOOST_DISABLE_ASSERTS)
#   pragma message("Warning: BOOST_DISABLE_ASSERTS not defined")
#endif

#include <boost/config.hpp>
#if defined(BOOST_MSVC)
#   pragma warning(disable: 4100 4127 4244 4512 4267)
#endif

#include "mapreduce.hpp"
#include <numeric>              // accumulate

#if defined(BOOST_MSVC)  && defined(_DEBUG)
#include <crtdbg.h>
#endif

namespace wordcount {

typedef
#ifdef WORD_COUNT_MEMORY_MAP_FILE
    std::pair<char const *, char const *>
#else
    std::ifstream
#endif
map_value_type;

template<typename T>
struct map_task
  : public mapreduce::map_task<std::string, map_value_type>
{
    template<typename Runtime>
    void operator()(Runtime &runtime, std::string const &key, T &value) const;
};

struct reduce_task : public mapreduce::reduce_task<std::string, unsigned>
{
    template<typename Runtime, typename It>
    void operator()(Runtime &runtime, std::string const &key, It it, It const ite) const
    {
        runtime.emit(key, std::accumulate(it, ite, 0));
    }
};

template<> template<typename Runtime>
void
map_task<
    std::pair<
        char const *, char const *> >::operator()(
            Runtime           &runtime,
            std::string const &/*key*/,
            std::pair<char const *, char const *> &value) const
{
    bool in_word = false;
    char const *ptr = value.first;
    char const *end = value.second;
    char const *word = ptr;
    for (; ptr != end; ++ptr)
    {
        char const ch = std::toupper(*ptr);
        if (in_word)
        {
            if ((ch < 'A' || ch > 'Z') && ch != '\'')
            {
                std::string w(word,ptr-word);
                std::transform(w.begin(), w.end(), w.begin(),
                               std::bind1st(
                                   std::mem_fun(&std::ctype<char>::tolower),
                                   &std::use_facet<std::ctype<char> >(std::locale::classic())));
                runtime.emit_intermediate(w, 1);
                in_word = false;
            }
        }
        else
        {
            if (ch >= 'A'  &&  ch <= 'Z')
            {
                word = ptr;
                in_word = true;
            }
        }
    }
    if (in_word)
    {
        BOOST_ASSERT(ptr-word > 0);
        std::string w(word,ptr-word);
        std::transform(w.begin(), w.end(), w.begin(),
                       std::bind1st(
                           std::mem_fun(&std::ctype<char>::tolower),
                           &std::use_facet<std::ctype<char> >(std::locale::classic())));
        runtime.emit_intermediate(w, 1);
    }
}


template<> template<typename Runtime>
void
map_task<std::ifstream>::operator()(
    Runtime            &runtime,
    std::string const  &/*key*/,
    std::ifstream      &value) const
{
    while (!value.eof())
    {
        std::string word;
        value >> word;
        std::transform(word.begin(), word.end(), word.begin(),
                       std::bind1st(
                           std::mem_fun(&std::ctype<char>::tolower),
                           &std::use_facet<std::ctype<char> >(std::locale::classic())));

        size_t length = word.length();
        size_t const original_length = length;
        std::string::const_iterator it;
        for (it=word.begin();
             it!=word.end()  &&  !std::isalnum(*it, std::locale::classic());
             ++it)
        {
            --length;
        }

        for (std::string::const_reverse_iterator rit=word.rbegin();
             length>0  &&  !std::isalnum(*rit, std::locale::classic());
             ++rit)
        {
            --length;
        }

        if (length > 0)
        {
            if (length == original_length)
                runtime.emit_intermediate(word, 1);
            else
                runtime.emit_intermediate(std::string(&*it,length), 1);
        }
    }
}

template<typename ReduceTask>
class combiner
{
  public:
    template<typename IntermediateStore>
    static void run(IntermediateStore &intermediate_store)
    {
        combiner instance;
        intermediate_store.combine(instance);
    }

    void start(typename ReduceTask::key_type const &)
    {
        total_ = 0;
    }

    template<typename IntermediateStore>
    void finish(typename ReduceTask::key_type const &key, IntermediateStore &intermediate_store)
    {
        if (total_ > 0)
            intermediate_store.insert(key, total_);
    }

    void operator()(typename ReduceTask::value_type const &value)
    {
        total_ += value;
    }

  private:
    combiner() { }

  private:
    unsigned total_;
};

typedef map_task<map_value_type> map_task_type;

}   // namespace wordcount


template<typename Job>
void run_test(mapreduce::specification spec)
{
    mapreduce::results result;

    std::cout << "\n" << typeid(Job).name() << "\n";

    try
    {
#ifdef RUN_SEQUENTIAL_MAP_REDUCE
        std::cout << "\nRunning Sequential MapReduce...";

        spec.map_tasks = 1;
        spec.reduce_tasks = 1;

        typename Job::datasource_type datasource(spec);
        Job job(datasource, spec);
        job.template run<mapreduce::schedule_policy::sequential<Job> >(result);
        std::cout << "\nSequential MapReduce Finished.";
#else
        std::cout << "\nRunning CPU Parallel MapReduce...";

        // this method can be called, but since we want access to the result data,
        // we need to have a job object to interrogate
        //mapreduce::run<Job>(spec, result);

        typename Job::datasource_type datasource(spec);
        Job job(datasource, spec);
        job.template run<mapreduce::schedule_policy::cpu_parallel<Job> >(result);
        std::cout << "\nCPU Parallel MapReduce Finished.\n";
#endif

        std::cout << "\nMapReduce statistics:";
        std::cout << "\n  MapReduce job runtime                     : " << result.job_runtime << " seconds, of which...";
        std::cout << "\n    Map phase runtime                       : " << result.map_runtime << " seconds";
        std::cout << "\n    Reduce phase runtime                    : " << result.reduce_runtime << " seconds";
        std::cout << "\n\n  Map:";
        std::cout << "\n    Total Map keys                          : " << result.counters.map_keys_executed;
        std::cout << "\n    Map keys processed                      : " << result.counters.map_keys_completed;
        std::cout << "\n    Map key processing errors               : " << result.counters.map_key_errors;
        std::cout << "\n    Number of Map Tasks run (in parallel)   : " << result.counters.actual_map_tasks;
        std::cout << "\n    Fastest Map key processed in            : " << *std::min_element(result.map_times.begin(), result.map_times.end()) << " seconds";
        std::cout << "\n    Slowest Map key processed in            : " << *std::max_element(result.map_times.begin(), result.map_times.end()) << " seconds";
        std::cout << "\n    Average time to process Map keys        : " << std::accumulate(result.map_times.begin(), result.map_times.end(), boost::posix_time::time_duration()) / result.map_times.size() << " seconds";

        std::cout << "\n\n  Reduce:";
        std::cout << "\n    Total Reduce keys                       : " << result.counters.reduce_keys_executed;
        std::cout << "\n    Reduce keys processed                   : " << result.counters.reduce_keys_completed;
        std::cout << "\n    Reduce key processing errors            : " << result.counters.reduce_key_errors;
        std::cout << "\n    Number of Reduce Tasks run (in parallel): " << result.counters.actual_reduce_tasks;
        std::cout << "\n    Number of Result Files                  : " << result.counters.num_result_files;
        if (result.reduce_times.size() > 0)
        {
            std::cout << "\n    Fastest Reduce key processed in         : " << *std::min_element(result.reduce_times.begin(), result.reduce_times.end()) << " seconds";
            std::cout << "\n    Slowest Reduce key processed in         : " << *std::max_element(result.reduce_times.begin(), result.reduce_times.end()) << " seconds";
            std::cout << "\n    Average time to process Reduce keys     : " << std::accumulate(result.reduce_times.begin(), result.reduce_times.end(), boost::posix_time::time_duration()) / result.map_times.size() << " seconds";
        }

        typename Job::const_result_iterator it  = job.begin_results();
        typename Job::const_result_iterator ite = job.end_results();
        if (it != ite)
        {
            typedef std::list<typename Job::keyvalue_t> frequencies_t;
            frequencies_t frequencies;
            frequencies.push_back(*it);
            typename frequencies_t::reverse_iterator it_smallest = frequencies.rbegin();
            for (++it; it!=ite; ++it)
            {
                if (frequencies.size() < 10)    // show top 10
                {
                    frequencies.push_back(*it);
                    if (it->second < it_smallest->second)
                        it_smallest = frequencies.rbegin();
                }
                else if (it->second > it_smallest->second)
                {
                    *it_smallest = *it;
                    it_smallest = std::min_element(frequencies.rbegin(), frequencies.rend(), mapreduce::detail::less_2nd<typename Job::keyvalue_t>);
                }
            }

            frequencies.sort(mapreduce::detail::greater_2nd<typename Job::keyvalue_t>);
            std::cout << "\n\nMapReduce results:";
            for (typename frequencies_t::const_iterator freq=frequencies.begin(); freq!=frequencies.end(); ++freq)
                std::cout << "\n" << freq->first << "\t" << freq->second;
        }
    }
    catch (std::exception &e)
    {
        std::cout << std::endl << "Error running MapReduce: " << e.what();
    }
}

std::istream &operator>>(std::istream &infile, std::pair<wordcount::reduce_task::key_type, wordcount::reduce_task::value_type> &kv_pair)
{
    mapreduce::intermediates::local_disk<
        wordcount::map_task_type,
        wordcount::reduce_task>::read_record(infile, kv_pair.first, kv_pair.second);
    return infile;
}

std::ostream &operator<<(std::ostream &o, std::pair<wordcount::reduce_task::key_type, wordcount::reduce_task::value_type> const &kv_pair)
{
    o << kv_pair.first.length() << "\t" << kv_pair.first << "\t" << kv_pair.second;
    return o;
}

int main(int argc, char **argv)
{
#if defined(BOOST_MSVC)  &&  defined(_DEBUG)
//    _CrtSetBreakAlloc(380);
    _CrtSetDbgFlag(_CrtSetDbgFlag(_CRTDBG_REPORT_FLAG) | _CRTDBG_LEAK_CHECK_DF);
#endif

    std::cout << "MapReduce test program";
    if (argc < 2)
    {
        std::cerr << "Usage: wordcount directory [num_map_tasks]\n";
        return 1;
    }

    mapreduce::specification spec;

    spec.input_directory = argv[1];

    if (argc > 2)
        spec.map_tasks = atoi(argv[2]);

    if (argc > 3)
        spec.reduce_tasks = atoi(argv[3]);
    else
        spec.reduce_tasks = std::max(1U,boost::thread::hardware_concurrency());

    std::cout << "\n" << std::max(1,(int)boost::thread::hardware_concurrency()) << " CPU cores";

    run_test<
        mapreduce::job<
            wordcount::map_task_type
          , wordcount::reduce_task>
        >(spec);

    run_test<
        mapreduce::job<
            wordcount::map_task_type
          , wordcount::reduce_task
          , wordcount::combiner<wordcount::reduce_task> >
        >(spec);

    run_test<
        mapreduce::job<
            wordcount::map_task_type
          , wordcount::reduce_task
          , wordcount::combiner<wordcount::reduce_task>
          , mapreduce::datasource::directory_iterator<wordcount::map_task_type>
          , mapreduce::intermediates::in_memory<wordcount::map_task_type, wordcount::reduce_task>
          , mapreduce::intermediates::reduce_file_output<wordcount::map_task_type, wordcount::reduce_task> >
        >(spec);

    run_test<
        mapreduce::job<
            wordcount::map_task_type
          , wordcount::reduce_task
          , wordcount::combiner<wordcount::reduce_task>
          , mapreduce::datasource::directory_iterator<wordcount::map_task_type>
          , mapreduce::intermediates::local_disk<wordcount::map_task_type, wordcount::reduce_task> >
        >(spec);

    return 0;
}
