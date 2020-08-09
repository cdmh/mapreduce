// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#pragma once

#include <iomanip>      // setw
#include "../job.hpp"
#ifdef __GNUC__
#include <iostream>     // ubuntu linux
#include <fstream>      // ubuntu linux
#endif

namespace mapreduce {

struct null_combiner;

namespace detail {

struct file_lines_comp
{
    template<typename T>
    bool const operator()(T const &first, T const &second)
    {
        return first.second < second.second;
    }
};

template<typename Record>
struct file_merger
{
    template<typename List>
    void operator()(List const &filenames, std::string const &dest)
    {
        std::copy(filenames.cbegin(), filenames.cend(), std::back_inserter(files));
        std::copy(filenames.cbegin(), filenames.cend(), std::back_inserter(delete_files));

        outfile.open(dest.c_str(), std::ios_base::out | std::ios_base::binary);
        while (files.size() > 0)
        {
            open_files();
            merge_files(dest);

            assert(file_lines.size() == 0);

            // subsequent merges need to merge with outfile from previous
            // iteration. to do this, rename the output file to a temporary
            // filename and add it to the list of files to merge, then re-open
            // the destination file
            if (files.size() > 0)
                rename_result_for_iterative_merge(dest);
        }
    }

  private:
    struct file_deleter : private std::vector<std::string>
    {
        ~file_deleter()
        {
            for (auto const &filename : *this)
                detail::delete_file(filename);
        }

        // enable std::back_inserter
        using std::vector<std::string>::push_back;
        using std::vector<std::string>::value_type;
        using std::vector<std::string>::const_reference;
    };

    void open_files()
    {
        // open each file and read the first record (line) from each
        while (files.size() > 0)
        {
            auto file = std::make_shared<std::ifstream>(files.front().c_str(), std::ios_base::in | std::ios_base::binary);
            if (!file->is_open())
                break;

            files.pop_front();

            std::string line;
            std::getline(*file, line, '\r');

            Record record;
            std::istringstream l(line);
            l >> record;
            file_lines.push_back(std::make_pair(file, record));
        }
    }

    void merge_files(std::string const &dest)
    {
        // find the smallest record in the list
        while (file_lines.size() > 0)
        {
            auto it = std::min_element(file_lines.begin(), file_lines.end(), file_lines_comp());

            // scan for all occurrences of the smallest record in our list of records,
            // and write each one to the output files
            Record const record = it->second;
            while (it != file_lines.end())
            {
                if (it->second == record)
                {
                    outfile << it->second << "\r";

                    std::string line;
                    std::getline(*it->first, line, '\r');

                    if (length(line) > 0)
                    {
                        std::istringstream l(line);
                        l >> it->second;
                    }

                    if (it->first->eof())
                    {
                        auto it1 = it++;
                        file_lines.erase(it1);
                    }
                    else
                        ++it;
                }
                else
                    ++it;
            }
        }
    }

    void rename_result_for_iterative_merge(std::string const &dest)
    {
        outfile.close();

        std::string const temp_filename = platform::get_temporary_filename();
        delete_file(temp_filename);
        boost::filesystem::rename(dest, temp_filename);
        delete_files.push_back(temp_filename);

        files.push_back(temp_filename);
        outfile.open(dest.c_str(), std::ios_base::out | std::ios_base::binary);
    }

  private:
    typedef std::list<std::pair<std::shared_ptr<std::ifstream>, Record> > file_lines_t;
    file_lines_t           file_lines;
    std::list<std::string> files;
    std::ofstream          outfile;
    file_deleter           delete_files;
};

template<typename Record>
struct file_key_combiner
{
    bool const operator()(std::string const &in, std::string const &out) const
    {
        return mapreduce::file_key_combiner<Record>(in, out);
    }
};

}   // namespace detail

namespace intermediates {

template<typename MapTask, typename ReduceTask>
class reduce_file_output
{
  public:
    reduce_file_output(std::string const &output_filespec,
                       size_t      const  partition,
                       size_t      const  num_partitions)
    {
        std::ostringstream filename;
        filename << output_filespec << partition+1 << "_of_" << num_partitions;
        filename_ = filename.str();
        output_file_.open(filename_.c_str(), std::ios_base::binary);
        if (!output_file_.is_open())
            throw std::runtime_error("Failed to open file " + filename_ );
    }

    void operator()(typename ReduceTask::key_type   const &key,
                    typename ReduceTask::value_type const &value)
    {
        output_file_ << key << "\t" << value << "\r";
    }

  private:
    std::string   filename_;
    std::ofstream output_file_;
};


template<typename T>
struct key_combiner : public T
{
    // the file_key_combiner will call this function to write multiple
    // occurances of a key/value pair to an output stream. the generic
    // case is to write the same key/value pair multiple times
    void write_multiple_values(std::ostream &out, size_t count)
    {
        if (count == 1)
            out << *this << "\r";
        else
        {
            std::ostringstream stream;
            stream << *this;
            std::string record = stream.str();
            for (size_t loop=0; loop<count; ++loop)
                out << record << "\r";
        }
    }
};


template<
    typename MapTask,
    typename ReduceTask,
    typename KeyType         = typename ReduceTask::key_type,
    typename PartitionFn     = hash_partitioner,
    typename StoreResultType = reduce_file_output<MapTask, ReduceTask>,
    typename CombineFile     = detail::file_key_combiner<key_combiner<std::pair<typename ReduceTask::key_type, typename ReduceTask::value_type>>>,
    typename MergeFn         = detail::file_merger<std::pair<typename ReduceTask::key_type, typename ReduceTask::value_type> > >
class local_disk : detail::noncopyable
{
  public:
    typedef MapTask         map_task_type;
    typedef ReduceTask      reduce_task_type;
    typedef KeyType         key_type;
    typedef StoreResultType store_result_type;

    typedef
    std::pair<
        typename reduce_task_type::key_type,
        typename reduce_task_type::value_type>
    keyvalue_t;

    class const_result_iterator
      : public boost::iterator_facade<
            const_result_iterator,
            keyvalue_t const,
            boost::forward_traversal_tag>
    {
        friend class boost::iterator_core_access;

      protected:
        explicit const_result_iterator(local_disk const *outer)
          : outer_(outer)
        {
            assert(outer_);
            kvlist_.resize(outer_->num_partitions_);
        }

        void increment()
        {
            if (!kvlist_[index_].first->eof())
                read_record(*kvlist_[index_].first, kvlist_[index_].second.first, kvlist_[index_].second.second);
            set_current();
        }

        bool const equal(const_result_iterator const &other) const
        {
            return (kvlist_.size() == 0  &&  other.kvlist_.size() == 0)
               ||  (kvlist_.size() > 0
               &&  other.kvlist_.size() > 0
               &&  kvlist_[index_].second == other.kvlist_[index_].second);
        }

        const_result_iterator &begin()
        {
            for (size_t loop=0; loop<outer_->num_partitions_; ++loop)
            {
                auto intermediate = outer_->intermediate_files_.find(loop);
                if (intermediate == outer_->intermediate_files_.end())
                    return end();

                kvlist_[loop] =
                    std::make_pair(
                        std::make_shared<std::ifstream>(
                            intermediate->second->filename.c_str(),
                            std::ios_base::binary),
                        keyvalue_t());

                assert(kvlist_[loop].first->is_open());
                read_record(
                    *kvlist_[loop].first,
                    kvlist_[loop].second.first,
                    kvlist_[loop].second.second);
            }
            set_current();
            return *this;
        }

        const_result_iterator &end()
        {
            index_ = 0;
            kvlist_.clear();
            return *this;
        }

        keyvalue_t const &dereference() const
        {
            return kvlist_[index_].second;
        }

        void set_current()
        {
            index_ = 0;
            while (index_<outer_->num_partitions_  &&  kvlist_[index_].first->eof())
                 ++index_;
            
            for (size_t loop=index_+1; loop<outer_->num_partitions_; ++loop)
            {
                if (!kvlist_[loop].first->eof()  &&  !kvlist_[index_].first->eof()  &&  kvlist_[index_].second > kvlist_[loop].second)
                    index_ = loop;
            }

            if (index_ == outer_->num_partitions_)
                end();
        }

      private:
        local_disk                    const *outer_;        // parent container
        size_t                               index_ = 0;    // index of current element
        typedef
        std::vector<
            std::pair<
                std::shared_ptr<std::ifstream>,
                keyvalue_t> >
        kvlist_t;
        kvlist_t kvlist_;

        friend class local_disk;
    };
    friend class const_result_iterator;

  private:
    struct intermediate_file_info
    {
        intermediate_file_info() = default;

        explicit intermediate_file_info(std::string const& fname)
          : filename(fname)
        {
        }

        struct kv_file : public std::ofstream
        {
            typedef typename MapTask::value_type    key_type;
            typedef typename ReduceTask::value_type value_type;

            kv_file() = default;

            ~kv_file()
            {
                close();
            }

            void open(std::string const &filename)
            {
                assert(records_.empty());
                use_cache_ = true;
                std::ofstream::open(filename.c_str(), std::ios_base::binary);
            }

            void close()
            {
                if (is_open())
                {
                    flush_cache();
                    std::ofstream::close();
                }
            }

            bool const sorted(void) const
            {
                return sorted_;
            }

            bool const write(key_type const &key, value_type const &value)
            {
                if (use_cache_)
                {
                    ++records_.insert(std::make_pair(std::make_pair(key,value),0U)).first->second;
                    return true;
                }

                sorted_ = false;
                return write(key, value, 1);
            }

          protected:
            bool const write(key_type   const &key,
                             value_type const &value,
                             size_t     const count)
            {
                std::ostringstream linebuf;
                linebuf << std::make_pair(key,value);

                std::string line(linebuf.str());
                for (size_t loop=0; loop<count; ++loop)
                {
                    *this << line << "\r";
                    if (fail())
                        return false;
                }
                return true;
            }

            bool const flush_cache()
            {
                use_cache_ = false;
                for (auto it  = records_.cbegin(); it != records_.cend(); ++it)
                {
                    if (!write(it->first.first, it->first.second, it->second))
                        return false;
                }

                records_.clear();
                return true;
            }

          private:
            using record_t  = std::pair<key_type, value_type>;
            using records_t = std::map<record_t, size_t>;

            bool      sorted_    = true;
            bool      use_cache_ = true;
            records_t records_;
        };

        std::string             filename;
        kv_file                 write_stream;
        std::list<std::string>  fragment_filenames;
    };

    typedef
    std::map<
        size_t, // hash value of intermediate key (R)
        std::shared_ptr<intermediate_file_info> >
    intermediates_t;

  public:
    explicit local_disk(size_t const num_partitions)
      : num_partitions_(num_partitions)
    {
    }

    ~local_disk()
    {
        try
        {
            this->close_files();

            // delete the temporary files
            for (auto it=intermediate_files_.cbegin();
                 it!=intermediate_files_.cend();
                 ++it)
            {
                intermediate_file_info const * const fileinfo = it->second.get();
                detail::delete_file(fileinfo->filename);
                for_each(
                    fileinfo->fragment_filenames.cbegin(),
                    fileinfo->fragment_filenames.cend(),
                    std::bind(detail::delete_file, std::placeholders::_1));
            }
        }
        catch (std::exception const &e)
        {
            std::cerr << "\nError: " << e.what() << "\n";
        }
    }

    const_result_iterator begin_results() const
    {
        return const_result_iterator(this).begin();
    }

    const_result_iterator end_results() const
    {
        return const_result_iterator(this).end();
    }

    // receive final result
    template<typename StoreResult>
    bool const insert(typename reduce_task_type::key_type   const &key,
                      typename reduce_task_type::value_type const &value,
                      StoreResult                                 &store_result)
    {
        store_result(key, value);
        return true;
    }

    // receive intermediate result
    bool const insert(key_type                     const &key,
                      typename reduce_task_type::value_type const &value)
    {
        size_t const partition = partitioner_(key, num_partitions_);

        auto it = intermediate_files_.find(partition);
        if (it == intermediate_files_.cend())
        {
            it = intermediate_files_.insert(
                    std::make_pair(
                        partition,
                        std::make_shared<intermediate_file_info>())).first;
        }

        if (it->second->filename.empty())
        {
            it->second->filename = platform::get_temporary_filename();
            assert(!it->second->write_stream.is_open());
        }

        if (!it->second->write_stream.is_open())
            it->second->write_stream.open(it->second->filename);
        assert(it->second->write_stream.is_open());
        return it->second->write_stream.write(key, value);
    }

    template<typename FnObj>
    void combine(FnObj &fn_obj)
    {
        using std::swap;

        this->close_files();
        for (auto it=intermediate_files_.cbegin(); it!=intermediate_files_.cend(); ++it)
        {
            std::string outfilename = platform::get_temporary_filename();

            // run the combine function to combine records with the same key
            auto &filename = it->second->filename;
            combine_fn_(filename, outfilename);
            detail::delete_file(filename);
            swap(filename, outfilename);
        }
        this->close_files();
    }

    void merge_from(local_disk &other)
    {
        assert(num_partitions_ == other.num_partitions_);
        for (size_t partition=0; partition<num_partitions_; ++partition)
        {
            auto ito = other.intermediate_files_.find(partition);
            if (ito != other.intermediate_files_.cend())
            {
                auto it = intermediate_files_.find(partition);
                if (it == intermediate_files_.cend())
                {
                    it = intermediate_files_.insert(
                            std::make_pair(
                                partition,
                                std::make_shared<intermediate_file_info>())).first;
                }

                ito->second->write_stream.close();
                if (ito->second->write_stream.sorted())
                {
                    it->second->fragment_filenames.push_back(ito->second->filename);
                    ito->second->filename.clear();
                }
                else
                {
                    std::string sorted = platform::get_temporary_filename();
                    combine_fn_(ito->second->filename, sorted);
                    it->second->fragment_filenames.push_back(sorted);
                }
                assert(ito->second->fragment_filenames.empty());
            }
        }
    }

    void run_intermediate_results_shuffle(size_t const partition)
    {
#ifdef DEBUG_TRACE_OUTPUT
        std::clog << "\nIntermediate Results Shuffle, Partition " << partition << "...";
#endif
        auto it = intermediate_files_.find(partition);
        assert(it != intermediate_files_.cend());
        it->second->write_stream.close();

        MergeFn merge_fn;
        if (!it->second->fragment_filenames.empty())
        {
            it->second->filename = platform::get_temporary_filename();
            merge_fn(it->second->fragment_filenames, it->second->filename);
        }
    }

    template<typename Callback>
    void reduce(size_t const partition, Callback &callback)
    {
#ifdef DEBUG_TRACE_OUTPUT
        std::clog << "\nReduce Phase running for partition " << partition << "...";
#endif

        auto it = intermediate_files_.find(partition);
        assert(it != intermediate_files_.cend());

        std::string filename;
        swap(filename, it->second->filename);
        it->second->write_stream.close();
        intermediate_files_.erase(it);

        std::pair<
            typename reduce_task_type::key_type,
            typename reduce_task_type::value_type> kv;
        typename reduce_task_type::key_type   last_key;
        std::list<typename reduce_task_type::value_type> values;
        std::ifstream infile(filename.c_str());
        while (!(infile >> kv).eof())
        {
            if (kv.first != last_key  &&  length(kv.first) > 0)
            {
                if (length(last_key) > 0)
                {
                    callback(last_key, values.cbegin(), values.cend());
                    values.clear();
                }
                if (length(kv.first) > 0)
                    swap(kv.first, last_key);
            }

            values.push_back(kv.second);
        }

        if (length(last_key) > 0)
            callback(last_key, values.cbegin(), values.cend());

        infile.close();
        detail::delete_file(filename.c_str());
    }

    static bool const read_record(std::istream &infile,
                                  typename reduce_task_type::key_type   &key,
                                  typename reduce_task_type::value_type &value)
    {
        std::pair<typename reduce_task_type::key_type,
                  typename reduce_task_type::value_type> keyvalue;
        infile >> keyvalue;
        if (infile.eof()  ||  infile.fail())
            return false;

        key   = keyvalue.first;
        value = keyvalue.second;
        return true;
    }

  private:
    void close_files()
    {
        for (auto it=intermediate_files_.cbegin(); it!=intermediate_files_.cend(); ++it)
            it->second->write_stream.close();
    }

  private:
    typedef enum { map_phase, reduce_phase } phase_t;

    size_t const    num_partitions_;
    intermediates_t intermediate_files_;
    CombineFile     combine_fn_;
    PartitionFn     partitioner_;
};

}   // namespace intermediates

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
