// Copyright (c) 2009-2013 Craig Henderson
// https://github.com/cdmh/mapreduce

#pragma once

#include <mutex>

namespace mapreduce {

namespace schedule_policy {

namespace detail {

template<typename Job>
inline void run_next_map_task(Job &job, std::mutex &m1, std::mutex &m2, results &result)
{
    try
    {
        bool run = true;
        while (run)
        {
            typename Job::map_task_type::key_type *key = 0;

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
inline void run_next_reduce_task(Job &job, unsigned &partition, std::mutex &mutex, results &result)
{
    try
    {
        // local lambda to increment the partition within a lock
        auto incr_partition = [&partition, &mutex] {
            std::lock_guard<std::mutex> guard(mutex);
            unsigned const part = partition++;
            return part;
        };

        while (1)
        {
            unsigned const part = incr_partition();
            if (part < job.number_of_partitions())
                job.run_reduce_task(part, result);
            else
                break;
        }
    }
    catch (std::exception &e)
    {
        std::cerr << "\nError: " << e.what() << "\n";
    }
}

template<typename Job>
void run_intermediate_results_shuffle(Job &job, unsigned const partition, results &result)
{
    try
    {
        auto const start_time = std::chrono::system_clock::now();
        job.run_intermediate_results_shuffle(partition);
        result.shuffle_times.push_back(std::chrono::system_clock::now() - start_time);
    }
    catch (std::exception &e)
    {
        std::cerr << "\nError: " << e.what() << "\n";
    }
}

}   // namespace detail


template<typename Job>
class cpu_parallel : mapreduce::detail::noncopyable
{
  public:
    cpu_parallel() : num_cpus_(std::max(1U, std::thread::hardware_concurrency()))
    {
    }

    void operator()(Job &job, results &result)
    {
        map(job, result);
        intermediate(job, result);
        reduce(job, result);
        collate_results(result);
        result.counters.num_result_files = job.number_of_partitions();
    }

  private:
    void map(Job &job, results &result)
    {
        std::mutex m1, m2;

        // run the Map Tasks
        auto     const start_time = std::chrono::system_clock::now();
        unsigned const map_tasks  = std::max(num_cpus_,std::min(num_cpus_, job.number_of_map_tasks()));

        mapreduce::detail::joined_thread_group map_threads;
        for (unsigned loop=0; loop<map_tasks; ++loop)
        {
            auto this_result = std::make_shared<results>();
            all_results_.push_back(this_result);

            map_threads.emplace_back(
                std::thread(
                    std::bind(
                        &detail::run_next_map_task<Job>,
                        std::ref(job),
                        std::ref(m1),
                        std::ref(m2),
                        std::ref(*this_result))));
        }
        map_threads.join_all();
        result.map_runtime = std::chrono::system_clock::now() - start_time;
        result.counters.actual_map_tasks = map_tasks;
    }

    void intermediate(Job &job, results &result)
    {
        // Intermediate results shuffle
        auto const start_time = std::chrono::system_clock::now();

        mapreduce::detail::joined_thread_group shuffle_threads;
        for (unsigned partition=0; partition<job.number_of_partitions(); ++partition)
        {
            for (unsigned loop=0;
                 loop<num_cpus_  &&  partition<job.number_of_partitions();
                 ++loop, ++partition)
            {
                auto this_result = std::make_shared<results>();
                all_results_.push_back(this_result);

                shuffle_threads.emplace_back(
                    std::thread(
                        std::bind(
                            &detail::run_intermediate_results_shuffle<Job>,
                            std::ref(job),
                            partition,
                            std::ref(*this_result))));
            }
        }
        shuffle_threads.join_all();
        result.shuffle_runtime = std::chrono::system_clock::now() - start_time;
    }

    void reduce(Job &job, results &result)
    {
        std::mutex m1;

        // run the Reduce Tasks
        mapreduce::detail::joined_thread_group reduce_threads;
        unsigned const reduce_tasks =
            std::min<unsigned const>(num_cpus_, job.number_of_partitions());

        auto const start_time(std::chrono::system_clock::now());

        unsigned partition = 0;
        for (unsigned loop=0; loop<reduce_tasks; ++loop)
        {
            auto this_result = std::make_shared<results>();
            all_results_.push_back(this_result);

            reduce_threads.emplace_back(
                std::thread(
                    std::bind(
                        &detail::run_next_reduce_task<Job>,
                        std::ref(job),
                        std::ref(partition),
                        std::ref(m1),
                        std::ref(*this_result))));
        }
        reduce_threads.join_all();
        result.reduce_runtime = std::chrono::system_clock::now() - start_time;
        result.counters.actual_reduce_tasks = reduce_tasks;
    }

    void collate_results(results &result)
    {
        // we're done with the map/reduce job, collate the statistics before returning
        for (all_results_t::const_iterator it=all_results_.begin();
             it!=all_results_.end();
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
                (*it)->shuffle_times.begin(),
                (*it)->shuffle_times.end(),
                std::back_inserter(result.shuffle_times));
            std::copy(
                (*it)->reduce_times.begin(),
                (*it)->reduce_times.end(),
                std::back_inserter(result.reduce_times));
        }
    }

  private:
    typedef std::vector<std::shared_ptr<results> > all_results_t;
    all_results_t all_results_;
    unsigned const num_cpus_;
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
