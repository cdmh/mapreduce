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

#ifndef MAPREDUCE_HASH_PARTITONER_HPP
#define MAPREDUCE_HASH_PARTITONER_HPP

#include <boost/functional/hash.hpp>
 
namespace mapreduce {

struct hash_partitioner
{
    template<typename T>
    size_t const operator()(T const &key, unsigned partitions) const
    {
        boost::hash<T> hasher;
        return hasher(key) % partitions;
    }
};

}   // namespace mapreduce

#endif  // MAPREDUCE_HASH_PARTITONER_HPP
