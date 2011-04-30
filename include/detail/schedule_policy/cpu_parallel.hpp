// MapReduce library
//
//  Copyright (C) 2009 Craig Henderson.
//  cdm.henderson@gmail.com
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// For more information, see http://craighenderson.co.uk/mapreduce/
//
 
#ifndef MAPREDUCE_CPU_PARALLEL_HPP
#define MAPREDUCE_CPU_PARALLEL_HPP

#include <boost/thread.hpp>

namespace mapreduce {

namespace schedule_policy {

namespace detail {

template<typename Job>
inline void run_next_map_task(Job &job, boost::mutex &m1, boost::mutex &m2, results &result)
{
    try
    {
        bool run = true;
        while (run)
        {
            void *key = 0;

            m1.lock();
            run = job.get_next_map_key(key);
            m1.unlock();

            if (run)
                job.run_map_task(key, result, m2);
        }
    }
    catch (std::exception &e)
    {
        std::cerr << "\nError: " << e.what() << "\n";
    }
}

template<typename Job>
inline void run_next_reduce_task(Job &job, unsigned &partition, boost::mutex &mutex, results &result)
{
    try
    {
        while (1)
        {
            boost::mutex::scoped_lock l(mutex);
            unsigned  part = partition++;
            if (part < job.number_of_partitions())
            {
                l.unlock();
                job.run_reduce_task(part, result);
            }
            else
                break;
        }
    }
    catch (std::exception &e)
    {
        std::cerr << "\nError: " << e.what() << "\n";
    }
}

}   // namespace detail


template<typename Job>
class cpu_parallel
{
  public:
    void operator()(Job &job, results &result)
    {
        unsigned const num_cpus = std::max(1U,boost::thread::hardware_concurrency());

        typedef std::vector<boost::shared_ptr<results> > all_results_t;
        all_results_t all_results;
        boost::mutex  m1, m2;

        // run the Map Tasks
        using namespace boost::posix_time;
        ptime start_time(microsec_clock::universal_time());

        unsigned const map_tasks = std::max(num_cpus,std::min(num_cpus, job.number_of_map_tasks()));

        boost::thread_group map_threads;
        for (unsigned loop=0; loop<map_tasks; ++loop)
        {
            boost::shared_ptr<results> this_result(new results);
            all_results.push_back(this_result);

            boost::thread *thread =
                new boost::thread(
                    detail::run_next_map_task<Job>,
                    boost::ref(job),
                    boost::ref(m1),
                    boost::ref(m2),
                    boost::ref(*this_result));
            map_threads.add_thread(thread);
        }
        map_threads.join_all();
        result.map_runtime = microsec_clock::universal_time() - start_time;

        // run the Reduce Tasks
        start_time = microsec_clock::universal_time();
        boost::thread_group reduce_threads;

        unsigned const reduce_tasks =
            std::min<unsigned const>(num_cpus, job.number_of_partitions());

        unsigned partition = 0;
        for (unsigned loop=0; loop<reduce_tasks; ++loop)
        {
            boost::shared_ptr<results> this_result(new results);
            all_results.push_back(this_result);

            boost::thread *thread =
                new boost::thread(
                    detail::run_next_reduce_task<Job>,
                    boost::ref(job),
                    boost::ref(partition),
                    boost::ref(m1),
                    boost::ref(*this_result));
            reduce_threads.add_thread(thread);
        }
        reduce_threads.join_all();
        result.reduce_runtime = microsec_clock::universal_time() - start_time;

        // we're done with the map/reduce job, collate the statistics before returning
        for (all_results_t::const_iterator it=all_results.begin();
             it!=all_results.end();
             ++it)
        {
            result.counters.map_keys_executed     += (*it)->counters.map_keys_executed;
            result.counters.map_key_errors        += (*it)->counters.map_key_errors;
            result.counters.map_keys_completed    += (*it)->counters.map_keys_completed;
            result.counters.reduce_keys_executed  += (*it)->counters.reduce_keys_executed;
            result.counters.reduce_key_errors     += (*it)->counters.reduce_key_errors;
            result.counters.reduce_keys_completed += (*it)->counters.reduce_keys_completed;

            std::copy(
                (*it)->map_times.begin(),
                (*it)->map_times.end(),
                std::back_inserter(result.map_times));
            std::copy(
                (*it)->reduce_times.begin(),
                (*it)->reduce_times.end(),
                std::back_inserter(result.reduce_times));

        }
        result.counters.actual_map_tasks    = map_tasks;
        result.counters.actual_reduce_tasks = reduce_tasks;
        result.counters.num_result_files    = job.number_of_partitions();
    }
};

}   // namespace schedule_policy

}   // namespace mapreduce 

#endif  // MAPREDUCE_CPU_PARALLEL_HPP
