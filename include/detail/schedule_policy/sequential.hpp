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
 
#ifndef MAPREDUCE_SEQUENTIAL_SCHEDULE_HPP
#define MAPREDUCE_SEQUENTIAL_SCHEDULE_HPP

namespace mapreduce {

namespace schedule_policy {

namespace detail {

struct null_lock
{
    void lock(void)   { }
    void unlock(void) { }
};

}   // namespace detail


template<typename Job>
class sequential
{
  public:
    void operator()(Job &job, results &result)
    {
        using namespace boost::posix_time;
        ptime start_time(microsec_clock::universal_time());

        // Map Tasks
        void *key = 0;
        detail::null_lock nolock;
        while (job.get_next_map_key(key)  &&  job.run_map_task(key, result, nolock))
            ;
        result.map_runtime = microsec_clock::universal_time() - start_time;

        // Reduce Tasks
        start_time = microsec_clock::universal_time();
        for (unsigned partition=0; partition<job.number_of_partitions(); ++partition)
            job.run_reduce_task(partition, result);
        result.reduce_runtime = microsec_clock::universal_time() - start_time;

        result.counters.actual_map_tasks    = 1;
        result.counters.actual_reduce_tasks = 1;
        result.counters.num_result_files    = job.number_of_partitions();
    }
};

}   // namespace schedule_policy

}   // namespace mapreduce 

#endif  // MAPREDUCE_SEQUENTIAL_SCHEDULE_HPP
