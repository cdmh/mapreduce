// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#pragma once

#include <boost/iterator/iterator_facade.hpp>

namespace mapreduce {

namespace intermediates {

template<typename ReduceKeyType, typename MapValueType>
inline ReduceKeyType make_intermediate_key(MapValueType const &value);

template<>
inline std::string make_intermediate_key(std::pair<char const *, std::uintmax_t> const &value)
{
    assert(value.second < std::numeric_limits<std::string::size_type>::max()); // assert on overflow
    return std::string(value.first, (std::string::size_type)value.second);
}

template<>
inline std::pair<char const *, std::uintmax_t> make_intermediate_key(std::string const &value)
{
    return std::make_pair(value.c_str(), value.length());
}

template<typename MapTask, typename ReduceTask>
class reduce_null_output
{
  public:
    reduce_null_output(std::string const &/*output_filespec*/,
                       size_t      const  /*partition*/,
                       size_t      const  /*num_partitions*/)
    {
    }

    bool const operator()(typename ReduceTask::key_type   const &/*key*/,
                          typename ReduceTask::value_type const &/*value*/)
    {
        return true;
    }
};


template<
    typename MapTask,
    typename ReduceTask,
    typename KeyType     = typename ReduceTask::key_type,
    typename PartitionFn = mapreduce::hash_partitioner,
    typename KeyCompare  = std::less<typename ReduceTask::key_type>,
    typename StoreResult = reduce_null_output<MapTask, ReduceTask>
>
class in_memory : detail::noncopyable
{
  public:
    typedef KeyType                         key_type;
    typedef typename ReduceTask::value_type value_type;
    typedef MapTask                         map_task_type;
    typedef ReduceTask                      reduce_task_type;
    typedef StoreResult                     store_result_type;

  private:
    typedef
    std::vector<
        std::map<
            KeyType, std::list<value_type>,KeyCompare>>
    intermediates_t;

  public:
    typedef
    std::pair<KeyType, value_type>
    keyvalue_t;

    class const_result_iterator
      : public boost::iterator_facade<
            const_result_iterator,
            keyvalue_t const,
            boost::forward_traversal_tag>
    {
        friend class boost::iterator_core_access;

      public:
        const_result_iterator(const_result_iterator const &) = default;

      private:
        explicit const_result_iterator(in_memory const *outer)
          : outer_(outer)
        {
            assert(outer_);
            iterators_.resize(outer_->num_partitions_);
        }

        const_result_iterator &operator=(const_result_iterator const &other);

        void increment()
        {
            ++current_.second;
            if (current_.second == iterators_[current_.first]->second.end())
            {
                if (iterators_[current_.first] != outer_->intermediates_[current_.first].end())
                    ++iterators_[current_.first];

                set_current();
            }
            else
                value_ = std::make_pair(iterators_[current_.first]->first, *current_.second);
        }

        bool const equal(const_result_iterator const &other) const
        {
            if (current_.first == std::numeric_limits<decltype(current_.first)>::max()  ||  other.current_.first == std::numeric_limits<decltype(current_.first)>::max())
                return other.current_.first == current_.first;
            return value_ == other.value_;
        }

        const_result_iterator &begin()
        {
            for (size_t loop=0; loop<outer_->num_partitions_; ++loop)
                iterators_[loop] = outer_->intermediates_[loop].cbegin();
            set_current();
            return *this;
        }

        const_result_iterator &end()
        {
            current_.first = std::numeric_limits<decltype(current_.first)>::max();
            value_ = keyvalue_t();
            iterators_.clear();
            return *this;
        }

        keyvalue_t const &dereference() const
        {
            return value_;
        }

        void set_current()
        {
            for (current_.first=0;
                 current_.first<outer_->num_partitions_  &&  iterators_[current_.first] == outer_->intermediates_[current_.first].end();
                 ++current_.first)
            { }
            
            for (auto loop=current_.first+1; loop<outer_->num_partitions_; ++loop)
            {
                if (iterators_[loop] != outer_->intermediates_[loop].end()  &&  *iterators_[current_.first] > *iterators_[loop])
                    current_.first = loop;
            }

            if (current_.first == outer_->num_partitions_)
                end();
            else
            {
                current_.second = iterators_[current_.first]->second.cbegin();
                value_ = std::make_pair(iterators_[current_.first]->first, *current_.second);
            }
        }

      private:
        typedef
        std::vector<typename intermediates_t::value_type::const_iterator>
        iterators_t;

        typedef
        std::pair<
            typename iterators_t::const_iterator,
            typename intermediates_t::value_type::mapped_type::const_iterator>
        current_t;

        keyvalue_t       value_;        // value of current element
        iterators_t      iterators_;    // iterator group
        in_memory const *outer_;        // parent container

        // the current element consists of an index to the partition
        // list, and an iterator within that list
        std::pair<
            size_t,                     // index of current element
            typename                    // iterator of the sub-element
                intermediates_t::value_type::mapped_type::const_iterator
        > current_;

        friend class in_memory;
    };
    friend class const_result_iterator;

    explicit in_memory(size_t const num_partitions=1)
      : num_partitions_(num_partitions)
    {
        intermediates_.resize(num_partitions_);
    }

    const_result_iterator begin_results() const
    {
        return const_result_iterator(this).begin();
    }

    const_result_iterator end_results() const
    {
        return const_result_iterator(this).end();
    }

    void swap(in_memory &other)
    {
        swap(intermediates_, other.intermediates_);
    }

    void run_intermediate_results_shuffle(size_t const /*partition*/)
    {
    }

    template<typename Callback>
    void reduce(size_t const partition, Callback &callback)
    {
        typename intermediates_t::value_type map;
        using std::swap;
        swap(map, intermediates_[partition]);

        for (auto const &result : map)
            callback(result.first, result.second.cbegin(), result.second.cend());
    }

    void merge_from(size_t partition, in_memory &other)
    {
        typedef typename intermediates_t::value_type map_type;

        map_type &map       = intermediates_[partition];
        map_type &other_map = other.intermediates_[partition];

        if (map.size() == 0)
        {
            using std::swap;
            swap(map, other_map);
            return;
        }

        for (auto const &result : other_map)
        {
            auto iti =
                map.insert(
                    std::make_pair(
                        result.first,
                        typename map_type::mapped_type())).first;

            std::copy(
                result.second.cbegin(),
                result.second.cend(),
                std::back_inserter(iti->second));
        }
    }

    void merge_from(in_memory &other)
    {
        for (size_t partition=0; partition<num_partitions_; ++partition)
            merge_from(partition, other);
        other.intermediates_.clear();
    }

    template<typename T>
    bool const insert(T const &key, typename reduce_task_type::value_type const &value)
    {
        return insert(make_intermediate_key<key_type>(key), value);
    }

    // receive final result
    bool const insert(typename reduce_task_type::key_type   const &key,
                      typename reduce_task_type::value_type const &value,
                      StoreResult &store_result)
    {
        return store_result(key, value)  &&  insert(key, value);
    }

    // receive intermediate result
    bool const insert(key_type                     const &key,
                      typename reduce_task_type::value_type const &value)
    {
        size_t const  partition = (num_partitions_ == 1)? 0 : partitioner_(key, num_partitions_);
        auto         &map       = intermediates_[partition];

        typedef typename intermediates_t::value_type::mapped_type mapped_type;
        map.insert(
            std::make_pair(
                key,
                mapped_type())).first->second.push_back(value);

        return true;
    }

    template<typename FnObj>
    void combine(FnObj &fn_obj)
    {
        intermediates_t intermediates;
        intermediates.resize(num_partitions_);
        using std::swap;
        swap(intermediates_, intermediates);

        for (auto const &intermediate : intermediates)
        {
            for (auto const &kv : intermediate)
            {
                fn_obj.start(kv.first);
                for (auto const &value : kv.second)
                    fn_obj(value);
                fn_obj.finish(kv.first, *this);
            }
        }
    }

    void combine(null_combiner &)
    {
    }

  private:
    size_t const    num_partitions_;
    intermediates_t intermediates_;
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
