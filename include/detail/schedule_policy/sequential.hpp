// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#pragma once

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
        map(job, result);
        intermediate(job, result);
        reduce(job, result);

        result.counters.actual_map_tasks    = 1;
        result.counters.actual_reduce_tasks = 1;
        result.counters.num_result_files    = job.number_of_partitions();
    }

    void map(Job &job, results &result)
    {
        auto const start_time(std::chrono::system_clock::now());

        typename Job::map_task_type::key_type *key = 0;
        detail::null_lock nolock;
        while (job.get_next_map_key(key)  &&  job.run_map_task(key, result, nolock))
            ;
        result.map_runtime = std::chrono::system_clock::now() - start_time;
    }

    void intermediate(Job &job, results &result)
    {
        auto const start_time(std::chrono::system_clock::now());
        for (size_t partition=0; partition<job.number_of_partitions(); ++partition)
            job.run_intermediate_results_shuffle(partition);
        result.shuffle_runtime = std::chrono::system_clock::now() - start_time;
    }

    void reduce(Job &job, results &result)
    {
        auto const start_time(std::chrono::system_clock::now());
        for (size_t partition=0; partition<job.number_of_partitions(); ++partition)
            job.run_reduce_task(partition, result);
        result.reduce_runtime = std::chrono::system_clock::now() - start_time;
    }
};

}   // namespace schedule_policy

}   // namespace mapreduce 

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
