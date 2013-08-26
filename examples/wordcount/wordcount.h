// MapReduce library
// Copyright (C) 2009-2013 Craig Henderson
// cdm.henderson@gmail.com

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
                    runtime.emit_intermediate(std::make_pair(word,ptr-word), 1);
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
            runtime.emit_intermediate(std::make_pair(word,ptr-word), 1);
        }
    }
};

struct reduce_task : public mapreduce::reduce_task<
                                std::pair<char const *, std::uintmax_t>,
                                unsigned>
{
    template<typename Runtime, typename It>
    void operator()(Runtime &runtime, key_type const &key, It it, It const ite) const
    {
        runtime.emit(key, std::accumulate(it, ite, 0));
    }
};

class combiner
{
  public:
    template<typename IntermediateStore>
    static void run(IntermediateStore &intermediate_store)
    {
        combiner instance;
        intermediate_store.combine(instance);
    }

    void start(reduce_task::key_type const &)
    {
        total_ = 0;
    }

    template<typename IntermediateStore>
    void finish(reduce_task::key_type const &key, IntermediateStore &intermediate_store)
    {
        if (total_ > 0)
            intermediate_store.insert(key, total_);
    }

    void operator()(reduce_task::value_type const &value)
    {
        total_ += value;
    }
        
  private:
    combiner() { }

  private:
    unsigned total_;
};

}   // namespace wordcount
