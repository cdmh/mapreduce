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

#ifndef MAPREDUCE_NULL_COMBINER_HPP
#define MAPREDUCE_NULL_COMBINER_HPP
 
namespace mapreduce {

struct null_combiner
{
    template<typename IntermediateStore>
    static void run(IntermediateStore &/*intermediate_store*/)
    {
    }

    template<typename IntermediateValueType>
    void start(IntermediateValueType const &)
    { }

    template<typename IntermediateValueType, typename IntermediateStore>
    void finish(IntermediateValueType const &, IntermediateStore &)
    { }

    template<typename IntermediateValueType>
    void operator()(IntermediateValueType const &)
    { }
};

}   // namespace mapreduce

#endif  // MAPREDUCE_NULL_COMBINER_HPP
