// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#pragma once

#ifdef BOOST_MSVC
#   if !defined(__SGI_STL_PORT)
#       if _MSC_VER < 1900
#           pragma message("warning: using STLPort is recommended to avoid STL container performance problems in MSVC supplied libraries.")
#       endif
#       if _SECURE_SCL == 1
#           pragma message("warning: using MSVC with _SECURE_SCL=1 defined can cause serious runtime performance degradation.")
#       endif
#   endif
#endif

#include <string>
#include <vector>
#include <thread>
#include <cstdint>
#include <boost/config.hpp>

namespace mapreduce {
namespace detail {

class noncopyable {
#if defined(_MSC_VER) && _MSC_VER < 1800
  protected:
    noncopyable() {}
    ~noncopyable() {}

  private:
    noncopyable(noncopyable const &);
    noncopyable(noncopyable &&);
    noncopyable &operator=(noncopyable const &);
    noncopyable &operator=(noncopyable &&);
#else
  protected:
    noncopyable()                               = default;
    ~noncopyable()                              = default;
    noncopyable(noncopyable const &)            = delete;
    noncopyable(noncopyable &&)                 = delete;
    noncopyable &operator=(noncopyable const &) = delete;
    noncopyable &operator=(noncopyable &&)      = delete;
#endif
};

class joined_thread_group : public std::vector<std::thread>
{
  public:
    ~joined_thread_group()
    {
        join_all();
    }

    void join_all()
    {
        for (auto &thread : *this)
        {
            if (thread.joinable())
                thread.join();
        }
    }
};


}   // namespace detail
}   // namespace mapreduce

namespace mapreduce {

struct specification
{
    size_t          map_tasks;             // ideal number of map tasks to use
    size_t          reduce_tasks;          // ideal number of reduce tasks to use
    std::string     output_filespec;       // filespec of the output files - can contain a directory path if required
    std::string     input_directory;       // directory path to scan for input files
    std::streamsize max_file_segment_size; // ideal maximum number of bytes in each input file segment

    specification()
      : map_tasks(0),                   
        reduce_tasks(1),
        max_file_segment_size(1048576L),    // default 1Mb
        output_filespec("mapreduce_")   
    {
    }
};

struct results
{
    struct tag_counters
    {
        size_t actual_map_tasks;        // number of map tasks actually used
        size_t actual_reduce_tasks;     // number of reduce tasks actually used

        // counters for map key processing
        size_t map_keys_executed;
        size_t map_key_errors;
        size_t map_keys_completed;

        // counters for reduce key processing
        size_t reduce_keys_executed;
        size_t reduce_key_errors;
        size_t reduce_keys_completed;

        size_t num_result_files;        // number of result files created

        tag_counters()
          : actual_map_tasks(0),
            actual_reduce_tasks(0),
            map_keys_executed(0),
            map_key_errors(0),
            map_keys_completed(0),
            reduce_keys_executed(0),
            reduce_key_errors(0),
            reduce_keys_completed(0),
            num_result_files(0)
        {
        }
    } counters;

    std::chrono::duration<double>              job_runtime;
    std::chrono::duration<double>              map_runtime;
    std::chrono::duration<double>              shuffle_runtime;
    std::chrono::duration<double>              reduce_runtime;
    std::vector<std::chrono::duration<double>> map_times;
    std::vector<std::chrono::duration<double>> shuffle_times;
    std::vector<std::chrono::duration<double>> reduce_times;
};

}   // namespace mapreduce

#include <boost/throw_exception.hpp>
#include "detail/platform.hpp"
#include "detail/mergesort.hpp"
#include "detail/null_combiner.hpp"
#include "detail/intermediates.hpp"
#include "detail/schedule_policy.hpp"
#include "detail/datasource.hpp"
#include "detail/job.hpp"

namespace mapreduce {

template<typename Job>
void run(mapreduce::specification &spec, mapreduce::results &result)
{
    typename Job::datasource_type datasource(spec);
    Job job(datasource, spec);
    job.template run<mapreduce::schedule_policy::cpu_parallel<Job> >(result);
}

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
