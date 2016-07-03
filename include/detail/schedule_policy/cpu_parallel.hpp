// Copyright (c) 2009-2016 Craig Henderson
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
inline void run_next_reduce_task(Job &job, size_t &partition, std::mutex &mutex, results &result)
{
    try
    {
        // local lambda to increment the partition within a lock
        auto incr_partition = [&partition, &mutex] {
            std::lock_guard<std::mutex> guard(mutex);
            size_t const part = partition++;
            return part;
        };

        while (1)
        {
            size_t const part = incr_partition();
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
void run_intermediate_results_shuffle(Job &job, size_t const partition, results &result)
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
        // run the Map Tasks
        auto   const start_time = std::chrono::system_clock::now();
        size_t const map_tasks  = std::max(size_t(num_cpus_), std::min(size_t(num_cpus_), job.number_of_map_tasks()));

        std::mutex m1, m2;
        mapreduce::detail::joined_thread_group map_threads;
        for (size_t loop=0; loop<map_tasks; ++loop)
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
        for (size_t partition=0; partition<job.number_of_partitions(); ++partition)
        {
            for (size_t loop=0;
                 loop<size_t(num_cpus_)  &&  partition<job.number_of_partitions();
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
        auto const start_time   = std::chrono::system_clock::now();
        auto const reduce_tasks = std::min(size_t(num_cpus_), job.number_of_partitions());

        size_t partition = 0;
        for (size_t loop=0; loop<reduce_tasks; ++loop)
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
        for (auto it=all_results_.cbegin(); it!=all_results_.cend(); ++it)
        {
            result.counters.map_keys_executed     += (*it)->counters.map_keys_executed;
            result.counters.map_key_errors        += (*it)->counters.map_key_errors;
            result.counters.map_keys_completed    += (*it)->counters.map_keys_completed;
            result.counters.reduce_keys_executed  += (*it)->counters.reduce_keys_executed;
            result.counters.reduce_key_errors     += (*it)->counters.reduce_key_errors;
            result.counters.reduce_keys_completed += (*it)->counters.reduce_keys_completed;

            std::copy(
                (*it)->map_times.cbegin(),
                (*it)->map_times.cend(),
                std::back_inserter(result.map_times));
            std::copy(
                (*it)->shuffle_times.cbegin(),
                (*it)->shuffle_times.cend(),
                std::back_inserter(result.shuffle_times));
            std::copy(
                (*it)->reduce_times.cbegin(),
                (*it)->reduce_times.cend(),
                std::back_inserter(result.reduce_times));
        }
    }

  private:
    typedef std::vector<std::shared_ptr<results> > all_results_t;
    all_results_t  all_results_;
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
