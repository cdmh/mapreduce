// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#pragma once

#include <boost/config.hpp>
#include <boost/system/system_error.hpp>

#if defined(BOOST_WINDOWS)
#include <windows.h>

#if defined(BOOST_MSVC)
#undef min
#undef max
#endif

#ifdef __GNUC__
#include <cstdlib>
#endif

namespace mapreduce {

namespace win32 {

namespace detail {

template<typename Char>
struct os_temporary_file_api_traits;

template<>
struct os_temporary_file_api_traits<char>
{
    static DWORD get_temp_path(DWORD length, char *buffer)
    {
        return GetTempPathA(length, buffer);
    }

    static unsigned get_temp_filename(char const *path, char const *prefix, unsigned unique, char *filename)
    {
        return GetTempFileNameA(path, prefix, unique, filename);
    }
};

template<>
struct os_temporary_file_api_traits<wchar_t>
{
    static DWORD get_temp_path(DWORD length, wchar_t *buffer)
    {
        return GetTempPathW(length, buffer);
    }

    static unsigned get_temp_filename(wchar_t const *path, wchar_t const *prefix, unsigned unique, wchar_t *filename)
    {
        return GetTempFileNameW(path, prefix, unique, filename);
    }
};

}   // namespace detail

template<typename Char>
std::basic_string<Char> &get_temporary_filename(std::basic_string<Char> &pathname)
{
    Char path[_MAX_PATH+1];
    if (!detail::os_temporary_file_api_traits<Char>::get_temp_path(sizeof(path)/sizeof(path[0]), path))
        BOOST_THROW_EXCEPTION(boost::system::system_error(GetLastError(), boost::system::system_category()));

    Char file[_MAX_PATH+1];
    if (!detail::os_temporary_file_api_traits<Char>::get_temp_filename(path, "mr_", 0, file))
        BOOST_THROW_EXCEPTION(boost::system::system_error(GetLastError(), boost::system::system_category()));

    pathname = file;
    return pathname;
}

#else

namespace mapreduce {

namespace linux_os {

std::string get_temporary_filename(std::string &pathname)
{
    const std::string tmp = "/tmp/XXXXXX";
    char* tmpfile = const_cast<char*>(tmp.c_str());
    auto tmp2 = mkstemp(tmpfile);
    std::string res(tmpfile);
    return res;
}

#endif

inline std::string const get_temporary_filename(void)
{
    std::string filename;
    return get_temporary_filename(filename);
}

}   // namespace win32/linux


#if defined(_WIN32)
namespace platform=win32;
#elif defined(__GNUC__)
namespace platform=linux_os;
#else
#error Undefined Platform
#endif

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
