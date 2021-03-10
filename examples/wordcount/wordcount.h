// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#include <numeric>      // accumulate

namespace wordcount {

struct map_task : public mapreduce::map_task<
                             std::string,                               // MapKey (filename)
                             std::pair<char const *, std::uintmax_t> >  // MapValue (memory mapped file contents)
{
    template<typename Runtime>
    void operator()(Runtime &runtime, key_type const &/*key*/, value_type &value) const
    {
        bool in_word = false;
        char const *ptr = value.first;
        char const *end = ptr + value.second;
        char const *word = ptr;
        for (; ptr != end; ++ptr)
        {
            char const ch = std::toupper(*ptr, std::locale::classic());
            if (in_word)
            {
                if ((ch < 'A' || ch > 'Z') && ch != '\'')
                {
                    runtime.emit_intermediate(std::pair<char const *, std::uintmax_t>(word,ptr-word), 1);
                    in_word = false;
                }
            }
            else if (ch >= 'A'  &&  ch <= 'Z')
            {
                word = ptr;
                in_word = true;
            }
        }

        if (in_word)
        {
            assert(ptr > word);
            runtime.emit_intermediate(std::pair<char const *, std::uintmax_t>(word,ptr-word), 1);
        }
    }
};

template<typename KeyType>
struct reduce_task : public mapreduce::reduce_task<KeyType, unsigned>
{
    template<typename Runtime, typename It>
    void operator()(Runtime &runtime, KeyType const &key, It it, It const ite) const
    {
        runtime.emit(key, std::accumulate(it, ite, 0));
    }
};

template<typename ReduceTask>
class combiner
{
  public:
    void start(typename ReduceTask::key_type const &)
    {
        total_ = 0;
    }

    template<typename IntermediateStore>
    void finish(typename ReduceTask::key_type const &key, IntermediateStore &intermediate_store)
    {
        if (total_ > 0)
        {
            // the combiner needs to emit an intermediate result, not a final result, so
            // here we convert the type from std::string (final) to intermediate (ptr/length)
            intermediate_store.insert(
                std::make_pair(
                    mapreduce::data(key),
                    mapreduce::length(key)),
                total_);
        }
    }

    void operator()(typename ReduceTask::value_type const &value)
    {
        total_ += value;
    }

  private:
    unsigned total_;
};


// Instead of writing
//  This  2
//  This  2
//  This  2
// we multiply the frequency by the the count and write:
//  This  6
template<typename T>
struct key_combiner : public T
{
    void write_multiple_values(std::ostream &out, unsigned count)
    {
        T temp(*this);
        temp.second *= count;
        out << temp << "\r";
    }
};

}   // namespace wordcount

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
