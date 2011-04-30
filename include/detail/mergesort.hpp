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
 
#ifndef MAPREDUCE_MERGESORT_HPP
#define MAPREDUCE_MERGESORT_HPP

#include <deque>
#include <list>
#include <map>
#include <sstream>
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>

#ifdef __GNUC__
#include <cstring> // ubuntu linux
#include <fstream> // ubuntu linux
#endif

#ifdef BOOST_NO_STDC_NAMESPACE
  namespace std { using ::strcmp; }
#endif

namespace mapreduce {

namespace detail {

template<typename T>
bool const less_2nd(T const &first, T const &second)
{
    return first.second < second.second;
}

template<typename T>
bool const greater_2nd(T const &first, T const &second)
{
    return first.second > second.second;
}

struct key_offset_compare
{
  public:
    key_offset_compare(unsigned const &offset)
      : offset_(offset)
    {
    }
    
    key_offset_compare(key_offset_compare const &other)
      : offset_(other.offset_)
    {
    }

    key_offset_compare &operator=(key_offset_compare const &other);

    template<typename T>
    bool const operator()(std::pair<T, std::string> const &first, std::pair<T, std::string> const &second) const
    {
        return (*this)(first.second, second.second);
    }

    bool const operator()(boost::shared_ptr<std::string> const &first, boost::shared_ptr<std::string> const &second) const
    {
        return (*this)(*first, *second);
    }

    bool const operator()(std::string const &first, std::string const &second) const
    {
        std::string::size_type length1 = first.length();
        std::string::size_type length2 = second.length();

        if (length1 == 0)
            return (length2 != 0);
        else if (length2 == 0)
            return (length1 == 0);
        else if (offset_ >= length1)
            return true;
        else if (offset_ >= length2)
            return false;

        std::string::const_iterator it1 = first.begin()  + offset_;
        std::string::const_iterator it2 = second.begin() + offset_;

        length1 -= offset_;
        length2 -= offset_;

        return (std::strcmp(&*it1, &*it2) < 0);
    }

  private:
    unsigned const &offset_;
};


template<typename It>
bool const do_file_merge(It first, It last, char const *outfilename, unsigned const offset)
{
#ifdef _DEBUG
    int const max_files=10;
#endif

    std::ofstream outfile(outfilename, std::ios_base::out | std::ios_base::binary);
    while (first!=last)
    {                     //!!!subsequent times around the loop need to merge with outfilename from previous iteration
        typedef std::list<std::pair<boost::shared_ptr<std::ifstream>, std::string> > file_lines_t;
        file_lines_t file_lines;
        for (; first!=last; ++first)
        {
            boost::shared_ptr<std::ifstream> file(new std::ifstream(first->c_str(), std::ios_base::in | std::ios_base::binary));
            if (!file->is_open())
                break;
#ifdef _DEBUG
            if (file_lines.size() == max_files)
                break;
#endif

            std::string line;
            std::getline(*file, line);
            file_lines.push_back(std::make_pair(file, line));
        }

        while (file_lines.size() > 0)
        {
            typename file_lines_t::iterator it;
            if (file_lines.size() == 1)
                it = file_lines.begin();
            else
                it = std::min_element(file_lines.begin(), file_lines.end(), detail::key_offset_compare(offset));
            outfile << it->second << "\n";

            std::getline(*it->first, it->second);
            if (it->first->eof())
                file_lines.erase(it);
        }
    }

    return true;
}

struct less_shared_ptr_string
{
    bool operator()(boost::shared_ptr<std::string> const &a, boost::shared_ptr<std::string> const &b) const
    {
        return *a < *b;
    }
};

inline bool const delete_file(std::string const &pathname)
{
    return boost::filesystem::remove(pathname);
}

template<typename Filenames>
class temporary_file_manager : boost::noncopyable
{
  public:
    temporary_file_manager(Filenames &filenames)
      : filenames_(filenames)
    {
    }

    ~temporary_file_manager()
    {
        try
        {
            // bind to the pass-though delete_file function because boost::filesystem::remove
            // takes a boost::filesystem::path parameter and not a std::string parameter - the
            // compiler will understandably not bind using an implicit conversion
            std::for_each(filenames_.begin(), filenames_.end(), boost::bind(delete_file, _1));
        }
        catch (std::exception &)
        {
        }
    }

  private:
    Filenames &filenames_;
};

}   // namespace detail



inline bool const merge_sort(char     const *in,
                             char     const *out,
                             unsigned const  offset,
                             unsigned const  max_lines = 1000000)
{
    std::deque<std::string>         temporary_files;
    detail::temporary_file_manager<
        std::deque<std::string> >   tfm(temporary_files);
    
    std::ifstream infile(in, std::ios_base::in | std::ios_base::binary);
    if (!infile.is_open())
    {
        std::ostringstream err;
        err << "Unable to open file " << in;
        BOOST_THROW_EXCEPTION(std::runtime_error(err.str()));
    }

    while (!infile.eof())
    {
        typedef std::map<boost::shared_ptr<std::string>, unsigned, detail::key_offset_compare> lines_t;
        detail::key_offset_compare map_comparator(offset);
        lines_t lines(map_comparator);

        for (unsigned loop=0; !infile.eof()  &&  loop<max_lines; ++loop)
        {
            if (infile.fail()  ||  infile.bad())
                BOOST_THROW_EXCEPTION(std::runtime_error("An error occurred reading the input file."));

            boost::shared_ptr<std::string> line(new std::string);
            std::getline(infile, *line);

            ++lines.insert(std::make_pair(line,0U)).first->second;
        }

        std::string const temp_filename(platform::get_temporary_filename());
        temporary_files.push_back(temp_filename);
        std::ofstream file(temp_filename.c_str(), std::ios_base::out | std::ios_base::binary);
        for (lines_t::const_iterator it=lines.begin(); it!=lines.end(); ++it)
        {
            if (file.fail()  ||  file.bad())
                BOOST_THROW_EXCEPTION(std::runtime_error("An error occurred writing temporary a file."));

            for (unsigned loop=0; loop<it->second; ++loop)
                file << *(it->first);
            file << "\n";
        }
    }
    infile.close();
    detail::do_file_merge(temporary_files.begin(), temporary_files.end(), out, offset);

	return true;
}

}   // namespace mapreduce

#endif  // MAPREDUCE_MERGESORT_HPP
