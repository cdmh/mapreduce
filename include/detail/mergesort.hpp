// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#pragma once

//#define DEBUG_TRACE_OUTPUT

#include <deque>
#include <list>
#include <map>
#include <sstream>
#include <fstream>
#include <iostream>
#include <boost/filesystem.hpp>

#ifdef __GNUC__
#include <cstring> // ubuntu linux
#include <fstream> // ubuntu linux
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

template<typename It>
bool const do_file_merge(It first, It last, std::string const &outfilename)
{
#ifdef _DEBUG
    int const max_files=10;
#endif

    int count = 0;
    std::ofstream outfile(outfilename.c_str(), std::ios_base::out | std::ios_base::binary);
    while (first!=last)
    {
        //!!!subsequent times around the loop need to merge with outfilename from previous iteration
        // in the meantime, we assert if we go round the loop more than once as it will produce incorrect results
        assert(++count == 1);

        typedef std::list<std::pair<std::shared_ptr<std::ifstream>, std::string> > file_lines_t;
        file_lines_t file_lines;
        for (; first!=last; ++first)
        {
            auto file = std::make_shared<std::ifstream>(first->c_str(), std::ios_base::in | std::ios_base::binary);
            if (!file->is_open())
                break;
#ifdef _DEBUG
            if (file_lines.size() == max_files)
                break;
#endif

            std::string line;
            std::getline(*file, line, '\r');
            file_lines.push_back(std::make_pair(file, line));
        }

        while (file_lines.size() > 0)
        {
            typename file_lines_t::iterator it;
            if (file_lines.size() == 1)
                it = file_lines.begin();
            else
                it = std::min_element(file_lines.begin(), file_lines.end());
            outfile << it->second << "\r";

            std::getline(*it->first, it->second, '\r');
            if (it->first->eof())
                file_lines.erase(it);
        }
    }

    return true;
}

inline bool const delete_file(std::string const &pathname)
{
    if (pathname.empty())
        return true;

    bool success = false;
    try
    {
#ifdef DEBUG_TRACE_OUTPUT
        std::clog << "\ndeleting " << pathname;
#endif
        success = boost::filesystem::remove(pathname);
    }
    catch (std::exception &e)
    {
        std::cerr << "Error deleting file \"" << pathname << "\"\n" << e.what() << "\n";
    }
    return success;
}

template<typename Filenames>
class temporary_file_manager : detail::noncopyable
{
  public:
    temporary_file_manager(Filenames &filenames)
      : filenames_(filenames)
    {
    }

    ~temporary_file_manager()
    {
        for (auto filename : filenames_)
        {
            try
            {
                delete_file(filename);
            }
            catch (std::exception &)
            {
            }
        }
    }

  private:
    Filenames &filenames_;
};

}   // namespace detail

template<typename T>
struct shared_ptr_indirect_less
{
    bool operator()(std::shared_ptr<T> const &left, std::shared_ptr<T> const &right) const
    {
        return *left < *right;
    }
};

template<typename Record>
bool const file_key_combiner(std::string const &in,
                             std::string const &out,
                             uint32_t    const  max_lines = 4294967000U)
{
#ifdef DEBUG_TRACE_OUTPUT
    std::clog << "\ncombining file keys " << in << "\n               into " << out;
#endif
    std::deque<std::string>         temporary_files;
    detail::temporary_file_manager<
        std::deque<std::string> >   tfm(temporary_files);
    
    std::ifstream infile(in.c_str(), std::ios_base::in | std::ios_base::binary);
    if (!infile.is_open())
    {
        std::ostringstream err;
        err << "Unable to open file " << in;
        BOOST_THROW_EXCEPTION(std::runtime_error(err.str()));
    }

    while (!infile.eof())
    {
        using lines_t = std::map<std::shared_ptr<Record>, std::streamsize, shared_ptr_indirect_less<Record>>;
        lines_t lines;

        for (uint32_t loop=0; !infile.eof()  &&  loop<max_lines; ++loop)
        {
            if (infile.fail())
                BOOST_THROW_EXCEPTION(std::runtime_error("An error occurred reading the input file."));

            std::string line;
            std::getline(infile, line, '\r');
            if (line.length() > 0) // ignore blank lines
            {
                auto record = std::make_shared<Record>();
                std::istringstream l(line);
                l >> *record;
                ++lines.insert(std::make_pair(record, std::streamsize())).first->second;
            }
        }

        std::string const temp_filename(platform::get_temporary_filename());
        temporary_files.push_back(temp_filename);
        std::ofstream file(temp_filename.c_str(), std::ios_base::out | std::ios_base::binary);
        for (auto it=lines.cbegin(); it!=lines.cend(); ++it)
        {
            if (file.fail())
                BOOST_THROW_EXCEPTION(std::runtime_error("An error occurred writing temporary a file."));

            it->first->write_multiple_values(file, it->second);
        }
    }
    infile.close();

    if (temporary_files.size() == 1)
    {
        detail::delete_file(out);
        boost::filesystem::rename(*temporary_files.cbegin(), out);
        temporary_files.clear();
    }
    else
        detail::do_file_merge(temporary_files.cbegin(), temporary_files.cend(), out);

	return true;
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
